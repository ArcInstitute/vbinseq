use std::io::Write;

use byteorder::{LittleEndian, WriteBytesExt};

use crate::error::Result;
use crate::header::{BlockHeader, VBinseqHeader};

/// Write a single flag to the writer.
pub fn write_flag<W: Write>(writer: &mut W, flag: u64) -> Result<()> {
    writer.write_u64::<LittleEndian>(flag)?;
    Ok(())
}

/// Write the sequence length to the writer.
pub fn write_length<W: Write>(writer: &mut W, length: u64) -> Result<()> {
    writer.write_u64::<LittleEndian>(length)?;
    Ok(())
}

/// Write all the elements of the embedded buffer to the writer.
pub fn write_buffer<W: Write>(writer: &mut W, ebuf: &[u64]) -> Result<()> {
    ebuf.iter()
        .try_for_each(|&x| writer.write_u64::<LittleEndian>(x))?;
    Ok(())
}

/// The record byte size is the size of the embedded buffer in bytes
/// as well as the size of the flag and length of the buffer.
///
/// S = wL + 2w
///
/// where S is the size of the record in bytes, L is the length of the buffer,
/// and w is the word size (1byte)
pub fn record_byte_size(ebuf: &[u64]) -> usize {
    (8 * ebuf.len()) + (2 * 8)
}

/// A writer for the VBinseq format.
///
/// The main intuition of VBinseq to initially write a header that describes the
/// internal block size of the format.
/// Then each block is preceded by a block header that acts as a marker to the start
/// of the block.
/// Each block is then filled with complete `Record`s until either the block is full
/// or no more complete `Record`s can be written to the block.
/// The remainder of the block is left empty, and the next block is started after the
/// length of the block.
///
/// The writing step is composed of two main steps:
/// 1. Check if the current block can handle the next `Record` and if not, write the
///   block header (at the appropriate position) and start a new block.
/// 2. Write the `Record` to the current block.
pub struct VBinseqWriter<W: Write> {
    /// Inner Writer
    inner: W,

    /// Header of the file
    header: VBinseqHeader,

    /// Reusable buffer for all nucleotides (written as 2-bit after conversion)
    sbuffer: Vec<u64>,

    /// Cursor position in block
    bpos: usize,

    /// Pre-initialized reusable padding buffer (filled with zero)
    padding: Vec<u8>,
}
impl<W: Write> VBinseqWriter<W> {
    pub fn new(inner: W, header: VBinseqHeader) -> Result<Self> {
        let mut wtr = Self {
            inner,
            header,
            sbuffer: Vec::new(),
            bpos: 0,
            padding: vec![0; header.block as usize],
        };
        wtr.init()?;
        Ok(wtr)
    }

    /// Initialize the writer by writing the header and the first block header.
    fn init(&mut self) -> Result<()> {
        self.header.write_bytes(&mut self.inner)?;
        self.write_block_header()?;
        Ok(())
    }

    fn write_block_header(&mut self) -> Result<()> {
        let block_header = BlockHeader::default();
        block_header.write_bytes(&mut self.inner)
    }

    fn flush_block(&mut self) -> Result<()> {
        // If the block is empty, do nothing
        if self.bpos == 0 {
            return Ok(());
        }
        let bytes_to_next_start = self.header.block as usize - self.bpos;
        self.inner.write_all(&self.padding[..bytes_to_next_start])?;
        self.bpos = 0;
        Ok(())
    }

    pub fn write_nucleotides(&mut self, flag: u64, sequence: &[u8]) -> Result<bool> {
        // encode the sequence
        self.sbuffer.clear();
        if bitnuc::encode(sequence, &mut self.sbuffer).is_err() {
            return Ok(false);
        }

        // Check if the current block can handle the next record
        // and initiate a new block if necessary
        let record_size = record_byte_size(&self.sbuffer);
        if self.bpos + record_size > self.header.block as usize {
            // eprintln!("Block full - starting new block");
            self.flush_block()?;
            self.write_block_header()?;
        } else {
            // let percent_full = (self.bpos as f64 / self.header.block as f64) * 100.0;
            // eprintln!(
            //     "Block at {percent_full}% - writing sequence length {}",
            //     sequence.len()
            // );
        }

        // Write the flag, length, and sequence to the block
        write_flag(&mut self.inner, flag)?;
        write_length(&mut self.inner, sequence.len() as u64)?;
        write_buffer(&mut self.inner, &self.sbuffer)?;

        // Update the block position
        self.bpos += record_size;

        Ok(true)
    }

    /// Finishes the internal writer.
    pub fn finish(&mut self) -> Result<()> {
        self.flush_block()?;
        self.inner.flush()?;
        Ok(())
    }
}

impl<W: Write> Drop for VBinseqWriter<W> {
    fn drop(&mut self) {
        self.finish()
            .expect("VBinseqWriter: Failed to finish writing");
    }
}
