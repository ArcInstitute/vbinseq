use anyhow::Result;
use vbinseq::BlockIndex;

pub fn main() -> Result<()> {
    let file = "./data/out.vbq";
    let index_path = format!("{file}.vqi");
    let index = BlockIndex::from_vbq(file)?;
    index.save_to_path(&index_path)?;
    eprintln!("Identified {} blocks", index.n_blocks());

    let new_index = BlockIndex::from_path(&index_path)?;
    println!("Found {} blocks in index", new_index.n_blocks());
    Ok(())
}
