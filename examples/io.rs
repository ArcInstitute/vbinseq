use std::{
    fs::File,
    io::{BufWriter, Read},
};

use anyhow::Result;
use paraseq::fastq;
use vbinseq::{reader::RecordBlock, MmapReader, VBinseqHeader, VBinseqWriter};

fn write_set(input_filepath: &str, output_filepath: &str) -> Result<()> {
    let in_handle = match_input(input_filepath)?;
    let mut reader = fastq::Reader::new(in_handle);
    let mut rset = fastq::RecordSet::default();

    let handle = File::create(output_filepath).map(BufWriter::new)?;
    let header = VBinseqHeader::new(true, true);
    let mut writer = VBinseqWriter::new(handle, header)?;

    let mut rnum = 0;
    while rset.fill(&mut reader)? {
        for record in rset.iter() {
            let record = record?;
            let seq = record.seq();
            let qual = record.qual();
            writer.write_nucleotides_quality(rnum, seq, qual)?;
            rnum += 1;
        }
    }
    eprintln!("Finished writing {} sequences to {}", rnum, output_filepath);

    Ok(())
}

fn read_set(filepath: &str) -> Result<()> {
    eprintln!("Reading sequences from {}", filepath);

    let mut reader = MmapReader::new(filepath)?;
    let mut block = RecordBlock::new();

    let mut n_records = 0;
    while reader.read_block_into(&mut block)? {
        n_records += block.n_records();
    }

    eprintln!("Read {} records", n_records);

    Ok(())
}

fn match_input(filepath: &str) -> Result<Box<dyn Read + Send>> {
    let (passthrough, _comp) = niffler::send::from_path(filepath)?;
    Ok(passthrough)
}

pub fn main() -> Result<()> {
    let in_filepath = "./data/out.fq.zst";
    let out_filepath = "./data/out.vbq";

    write_set(in_filepath, out_filepath)?;
    read_set(out_filepath)?;

    Ok(())
}
