use std::{fs::File, io::BufWriter};

use anyhow::Result;
use nucgen::Sequence;
use rand::Rng;
use vbinseq::{VBinseqHeader, VBinseqWriter};

pub fn main() -> Result<()> {
    let filepath = "./test.vbq";
    let num_seq = 10_000;
    let lbound = 500; // smallest sequence size
    let rbound = 20000; // largest sequence size
    let mut seq = Sequence::with_capacity(rbound);
    let mut rng = rand::thread_rng();
    seq.fill_buffer(&mut rng, rbound);

    let handle = File::create(filepath).map(BufWriter::new)?;
    let header = VBinseqHeader::default();
    let mut writer = VBinseqWriter::new(handle, header)?;

    for idx in 0..num_seq {
        // Generate a random sequence size
        seq.fill_buffer(&mut rng, rbound);

        if idx % 1_000 == 0 {
            eprintln!("Processed {} sequences", idx);
        }

        let size = rng.gen_range(lbound..=rbound);
        let seq_buffer = seq.bytes();
        let subseq = &seq_buffer[..size];
        writer.write_nucleotides(1 + idx as u64, subseq)?;
    }
    eprintln!("Finished writing {} sequences to {}", num_seq, filepath);

    Ok(())
}
