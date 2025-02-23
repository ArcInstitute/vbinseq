use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use byteorder::{ByteOrder, LittleEndian};
use memmap2::Mmap;

use crate::{
    error::ReadError,
    header::{SIZE_BLOCK_HEADER, SIZE_HEADER},
    BlockHeader, Result, VBinseqHeader,
};

fn encoded_sequence_len(len: u64) -> usize {
    len.div_ceil(32) as usize
}

pub struct RecordBlock {
    /// Buffer: All flags in the block
    flags: Vec<u64>,

    /// Buffer: All lengths in the block
    lens: Vec<u64>,

    /// Buffer: All packed sequences in the block
    sequences: Vec<u64>,
}
impl RecordBlock {
    pub fn new() -> Self {
        Self {
            flags: Vec::new(),
            lens: Vec::new(),
            sequences: Vec::new(),
        }
    }

    pub fn n_records(&self) -> usize {
        self.flags.len()
    }

    pub fn iter(&self) -> RecordBlockIter {
        RecordBlockIter::new(self)
    }

    pub fn clear(&mut self) {
        self.flags.clear();
        self.lens.clear();
        self.sequences.clear();
    }

    fn ingest_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let mut pos = 0;
        loop {
            // Check that we have enough bytes to at least read the flag
            // and length. If not, break out of the loop.
            if pos + 16 > bytes.len() {
                break;
            }

            // Read the flag and advance the position
            let flag = LittleEndian::read_u64(&bytes[pos..pos + 8]);
            pos += 8;

            // Read the length and advance the position
            let len = LittleEndian::read_u64(&bytes[pos..pos + 8]);
            pos += 8;

            // No more records in the block
            if len == 0 {
                // It is possible to end up here if the block is not full
                // In this case the flag and the length are both zero
                // and effectively blank but initialized memory.
                break;
            }

            // Add the record to the block
            self.flags.push(flag);
            self.lens.push(len);

            let mut seq = [0u8; 8];
            for _ in 0..encoded_sequence_len(len) {
                seq.copy_from_slice(&bytes[pos..pos + 8]);
                self.sequences.push(LittleEndian::read_u64(&seq));
                pos += 8;
            }
        }
        Ok(())
    }
}

pub struct RecordBlockIter<'a> {
    block: &'a RecordBlock,
    /// Record position in the block
    rpos: usize,
    /// Encoded sequence position in the block
    epos: usize,
}
impl<'a> RecordBlockIter<'a> {
    pub fn new(block: &'a RecordBlock) -> Self {
        Self {
            block,
            rpos: 0,
            epos: 0,
        }
    }
}
impl<'a> Iterator for RecordBlockIter<'a> {
    type Item = RefRecord<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rpos == self.block.n_records() {
            return None;
        }
        let flag = self.block.flags[self.rpos];
        let len = self.block.lens[self.rpos];
        let elen = encoded_sequence_len(len);
        let sequence = &self.block.sequences[self.epos..self.epos + elen];

        self.rpos += 1;
        self.epos += elen;

        Some(RefRecord::new(flag, len, sequence))
    }
}

pub struct RefRecord<'a> {
    flag: u64,
    len: u64,
    sequence: &'a [u64],
}
impl<'a> RefRecord<'a> {
    pub fn new(flag: u64, len: u64, sequence: &'a [u64]) -> Self {
        Self {
            flag,
            len,
            sequence,
        }
    }
    pub fn flag(&self) -> u64 {
        self.flag
    }
    pub fn len(&self) -> u64 {
        self.len
    }
    pub fn sequence(&self) -> &[u64] {
        self.sequence
    }
    pub fn decode_into(&self, dbuf: &mut Vec<u8>) -> Result<()> {
        bitnuc::decode(self.sequence, self.len as usize, dbuf)?;
        Ok(())
    }
}

pub struct MmapReader {
    /// Memory mapped file contents
    mmap: Arc<Mmap>,

    /// Header information
    header: VBinseqHeader,

    /// Cursor position in the file
    pos: usize,
}
impl MmapReader {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        // Verify it's a regular file before attempting to map
        let file = File::open(path)?;
        if !file.metadata()?.is_file() {
            return Err(ReadError::InvalidFileType.into());
        }

        // Safety: The file is open and won't be modified while mapped
        let mmap = unsafe { Mmap::map(&file)? };

        // Read header from mapped memory
        let header = {
            let mut header_bytes = [0u8; SIZE_HEADER];
            header_bytes.copy_from_slice(&mmap[..SIZE_HEADER]);
            VBinseqHeader::from_bytes(&header_bytes)?
        };

        Ok(Self {
            mmap: Arc::new(mmap),
            header,
            pos: SIZE_HEADER,
        })
    }

    /// Fill an existing RecordBlock with the next block of records
    ///
    /// Returns false if EOF was reached, true if the block was filled
    pub fn read_block_into(&mut self, block: &mut RecordBlock) -> Result<bool> {
        // Clear the block
        block.clear();

        // Validate the next block header is within bounds and present
        if self.pos + SIZE_BLOCK_HEADER > self.mmap.len() {
            return Ok(false);
        }
        BlockHeader::validate(&self.mmap, self.pos)?; // validate the block header
        self.pos += SIZE_BLOCK_HEADER; // advance past the block header

        // Read the block contents
        if self.pos + self.header.block as usize > self.mmap.len() {
            return Err(ReadError::UnexpectedEndOfFile(self.pos).into());
        }
        let block_buffer = &self.mmap[self.pos..self.pos + self.header.block as usize];
        block.ingest_bytes(block_buffer)?;
        self.pos += self.header.block as usize; // advance past the block contents

        Ok(true)
    }
}
