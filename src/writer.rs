use std::io::Write;

use byteorder::{LittleEndian, WriteBytesExt};

use crate::error::Result;
use crate::VBinseqHeader;

/// Write a single flag to the writer.
pub fn write_flag<W: Write>(writer: &mut W, flag: u64) -> Result<()> {
    writer.write_u64::<LittleEndian>(flag)?;
    Ok(())
}

/// Write all the elements of the embedded buffer to the writer.
pub fn write_buffer<W: Write>(writer: &mut W, ebuf: &[u64]) -> Result<()> {
    ebuf.iter()
        .try_for_each(|&x| writer.write_u64::<LittleEndian>(x))?;
    Ok(())
}

pub struct VBinseqWriter<W: Write> {
    /// Inner Writer
    inner: W,

    /// Header of the file
    header: VBinseqHeader,

    /// Reusable buffer for all nucleotides (written as 2-bit after conversion)
    sbuffer: Vec<u64>,

    /// Cursor position in block
    bpos: usize,
}
impl<W: Write> VBinseqWriter<W> {
    pub fn new(mut inner: W, header: VBinseqHeader) -> Result<Self> {
        header.write_bytes(&mut inner)?;
        Ok(Self {
            inner,
            header,
            sbuffer: Vec::new(),
            bpos: 0,
        })
    }

    fn write_block_header(&mut self) -> Result<()> {
        Ok(())
    }

    pub fn write_nucleotides(&mut self, flag: u64, sequence: &[u8]) -> Result<bool> {
        Ok(true)
    }
}
