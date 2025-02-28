use std::{
    fs::File,
    io::{stdout, BufWriter, Read, Write},
};

use anyhow::Result;
use clap::Parser;
use paraseq::fastq;
use vbinseq::{MmapReader, VBinseqHeader, VBinseqWriter};

#[derive(Parser)]
struct Args {
    #[clap(short, long, default_value = "./data/out.fq.zst")]
    input: String,
    #[clap(short, long, default_value = "./data/out.vbq")]
    output: String,
    #[clap(short, long)]
    compress: bool,
    #[clap(short = 'q', long)]
    write_quality: bool,
    #[clap(short = 'p', long)]
    paired: bool,
    #[clap(short = 's', long)]
    skip_write: bool,
    #[clap(short = 'S', long)]
    skip_read: bool,
}

fn write_set(
    input_filepath: &str,
    output_filepath: &str,
    compress: bool,
    write_quality: bool,
) -> Result<()> {
    let in_handle = match_input(input_filepath)?;
    let mut reader = fastq::Reader::new(in_handle);
    let mut rset = fastq::RecordSet::default();

    eprintln!(
        "Writing sequences to {} (compress: {}, with_quality: {})",
        output_filepath, compress, write_quality
    );
    let handle = File::create(output_filepath).map(BufWriter::new)?;
    let header = VBinseqHeader::new(write_quality, compress, false);
    let mut writer = VBinseqWriter::new(handle, header)?;

    let mut rnum = 0;
    while rset.fill(&mut reader)? {
        for record in rset.iter() {
            let record = record?;
            let seq = record.seq();
            let qual = record.qual();
            if write_quality {
                writer.write_nucleotides_quality(rnum, seq, qual)?;
            } else {
                writer.write_nucleotides(rnum, seq)?;
            }
            rnum += 1;
        }
    }
    eprintln!("Finished writing {} sequences to {}", rnum, output_filepath);

    Ok(())
}

fn write_paired_set(
    input_filepath: &str,
    output_filepath: &str,
    compress: bool,
    write_quality: bool,
) -> Result<()> {
    let in_handle = match_input(input_filepath)?;
    let mut reader = fastq::Reader::new(in_handle);
    let mut rset = fastq::RecordSet::default();

    eprintln!(
        "Writing sequences to {} (compress: {}, with_quality: {})",
        output_filepath, compress, write_quality
    );
    let handle = File::create(output_filepath).map(BufWriter::new)?;
    let header = VBinseqHeader::new(write_quality, compress, true);
    let mut writer = VBinseqWriter::new(handle, header)?;

    let mut rnum = 0;
    while rset.fill(&mut reader)? {
        for record in rset.iter() {
            let record = record?;
            let seq = record.seq();
            let qual = record.qual();
            if write_quality {
                writer.write_nucleotides_quality_paired(rnum, seq, seq, qual, qual)?;
            } else {
                writer.write_nucleotides_paired(rnum, seq, seq)?;
            }
            rnum += 1;
        }
    }
    eprintln!("Finished writing {} sequences to {}", rnum, output_filepath);

    Ok(())
}

fn read_set(filepath: &str) -> Result<()> {
    eprintln!("Reading sequences from {}", filepath);

    let mut writer = BufWriter::new(stdout());
    let mut reader = MmapReader::new(filepath)?;
    let mut block = reader.new_block();

    let mut n_records = 0;
    let mut n_blocks = 0;
    let mut dbuf = Vec::new();
    let mut qbuf = Vec::new();
    while reader.read_block_into(&mut block)? {
        for record in block.iter() {
            record.decode_s(&mut dbuf)?;

            let seq_str = std::str::from_utf8(&dbuf)?;

            if record.squal().is_empty() {
                // write dummy quality scores
                qbuf.resize(dbuf.len(), b'?');
                let qual_str = std::str::from_utf8(&qbuf)?;
                writeln!(
                    &mut writer,
                    "@seq.{}\n{}\n+\n{}",
                    n_records, seq_str, qual_str
                )?;
            } else {
                let qual_str = std::str::from_utf8(record.squal())?;
                writeln!(
                    &mut writer,
                    "@seq.{}\n{}\n+\n{}",
                    n_records, seq_str, qual_str
                )?;
            }
            dbuf.clear();
            n_records += 1;
        }
        n_blocks += 1;
        // eprintln!("Read {} records from block {}", block.n_records(), n_blocks);
    }
    writer.flush()?;

    eprintln!("Read {} records from {} blocks", n_records, n_blocks);
    Ok(())
}

fn read_paired_set(filepath: &str) -> Result<()> {
    eprintln!("Reading sequences from {}", filepath);

    let mut writer = BufWriter::new(stdout());
    let mut reader = MmapReader::new(filepath)?;
    let mut block = reader.new_block();

    let mut n_records = 0;
    let mut n_blocks = 0;
    let mut sbuf = Vec::new();
    let mut xbuf = Vec::new();
    let mut squal = Vec::new();
    let mut xqual = Vec::new();
    while reader.read_block_into(&mut block)? {
        for record in block.iter() {
            record.decode_s(&mut sbuf)?;
            record.decode_x(&mut xbuf)?;

            let s_seq_str = std::str::from_utf8(&sbuf)?;
            let x_seq_str = std::str::from_utf8(&xbuf)?;

            if record.squal().is_empty() {
                // write dummy quality scores
                squal.resize(sbuf.len(), b'?');
                xqual.resize(xbuf.len(), b'?');
                let s_qual_str = std::str::from_utf8(&squal)?;
                let x_qual_str = std::str::from_utf8(&xqual)?;
                writeln!(
                    &mut writer,
                    "@seq.{}/1\n{}\n+\n{}",
                    n_records, s_seq_str, s_qual_str
                )?;
                writeln!(
                    &mut writer,
                    "@seq.{}/2\n{}\n+\n{}",
                    n_records, x_seq_str, x_qual_str
                )?;
            } else {
                let s_qual_str = std::str::from_utf8(record.squal())?;
                let x_qual_str = std::str::from_utf8(record.xqual())?;
                writeln!(
                    &mut writer,
                    "@seq.{}/1\n{}\n+\n{}",
                    n_records, s_seq_str, s_qual_str
                )?;
                writeln!(
                    &mut writer,
                    "@seq.{}/2\n{}\n+\n{}",
                    n_records, x_seq_str, x_qual_str
                )?;
            }
            sbuf.clear();
            xbuf.clear();
            n_records += 1;
        }
        n_blocks += 1;
        // eprintln!("Read {} records from block {}", block.n_records(), n_blocks);
    }
    writer.flush()?;

    eprintln!("Read {} records from {} blocks", n_records, n_blocks);
    Ok(())
}

fn match_input(filepath: &str) -> Result<Box<dyn Read + Send>> {
    let (passthrough, _comp) = niffler::send::from_path(filepath)?;
    Ok(passthrough)
}

pub fn main() -> Result<()> {
    let args = Args::parse();
    if !args.skip_write {
        if args.paired {
            write_paired_set(&args.input, &args.output, args.compress, args.write_quality)?;
        } else {
            write_set(&args.input, &args.output, args.compress, args.write_quality)?;
        }
    }
    if !args.skip_read {
        if args.paired {
            read_paired_set(&args.output)?;
        } else {
            read_set(&args.output)?;
        }
    }
    Ok(())
}
