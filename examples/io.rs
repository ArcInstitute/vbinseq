use std::{
    fs::File,
    io::{BufWriter, Read},
};

use anyhow::Result;
use paraseq::fastq;
use vbinseq::{MmapReader, VBinseqHeader, VBinseqWriter};

fn write_set(input_filepath: &str, output_filepath: &str) -> Result<()> {
    let write_quality = true;
    let compress = true;

    let in_handle = match_input(input_filepath)?;
    let mut reader = fastq::Reader::new(in_handle);
    let mut rset = fastq::RecordSet::default();

    eprintln!(
        "Writing sequences to {} (compress: {}, with_quality: {})",
        output_filepath, compress, write_quality
    );
    let handle = File::create(output_filepath).map(BufWriter::new)?;
    let header = VBinseqHeader::new(write_quality, compress);
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

fn read_set(filepath: &str) -> Result<()> {
    eprintln!("Reading sequences from {}", filepath);

    let mut reader = MmapReader::new(filepath)?;
    let mut block = reader.new_block();

    let mut n_records = 0;
    let mut n_blocks = 0;
    let mut dbuf = Vec::new();
    while reader.read_block_into(&mut block)? {
        n_records += block.n_records();
        for record in block.iter() {
            record.decode_into(&mut dbuf)?;
            dbuf.clear();
        }
        n_blocks += 1;
        eprintln!("Read {} records from block {}", block.n_records(), n_blocks);
    }

    eprintln!("Read {} records from {} blocks", n_records, n_blocks);
    Ok(())
}

fn match_input(filepath: &str) -> Result<Box<dyn Read + Send>> {
    let (passthrough, _comp) = niffler::send::from_path(filepath)?;
    Ok(passthrough)
}

pub fn main() -> Result<()> {
    let in_filepath = std::env::args()
        .nth(1)
        .unwrap_or("./data/out.fq.zst".to_string());
    let out_filepath = std::env::args()
        .nth(2)
        .unwrap_or("./data/out.vbq".to_string());

    write_set(&in_filepath, &out_filepath)?;
    read_set(&out_filepath)?;

    Ok(())
}
