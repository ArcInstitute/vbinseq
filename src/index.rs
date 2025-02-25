use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

use byteorder::{ByteOrder, LittleEndian};
use zstd::{Decoder, Encoder};

use crate::{
    header::{SIZE_BLOCK_HEADER, SIZE_HEADER},
    BlockHeader, Result, VBinseqHeader,
};

/// Size of BlockRange in bytes
pub const SIZE_BLOCK_RANGE: usize = 16;

/// Descriptor of the dimensions of a Block
#[derive(Debug, Clone, Copy)]
pub struct BlockRange {
    /// File offset where the block starts (including block header + file header)
    pub start_offset: u64,
    /// Actual size of the block
    pub len: u64,
}
impl BlockRange {
    pub fn new(start_offset: u64, len: u64) -> Self {
        Self { start_offset, len }
    }

    /// Write self into a write handle
    pub fn write_bytes<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut buf = [0; SIZE_BLOCK_RANGE];
        LittleEndian::write_u64(&mut buf[0..8], self.start_offset);
        LittleEndian::write_u64(&mut buf[8..16], self.len);
        writer.write_all(&buf)?;
        Ok(())
    }

    /// Read self from exact byte buffer
    pub fn from_exact(buffer: &[u8; SIZE_BLOCK_RANGE]) -> Self {
        Self {
            start_offset: LittleEndian::read_u64(&buffer[0..8]),
            len: LittleEndian::read_u64(&buffer[8..16]),
        }
    }

    /// Read self from byte buffer
    pub fn from_bytes(buffer: &[u8]) -> Self {
        let mut buf = [0; SIZE_BLOCK_RANGE];
        buf.copy_from_slice(buffer);
        Self::from_exact(&buf)
    }
}

/// Collection of block ranges forming an index
#[derive(Debug, Clone, Default)]
pub struct BlockIndex {
    ranges: Vec<BlockRange>,
}
impl BlockIndex {
    pub fn n_blocks(&self) -> usize {
        self.ranges.len()
    }

    /// Writes the collection of BlockRange to a file
    pub fn save_to_path<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let writer = File::create(path).map(BufWriter::new)?;
        let mut writer = Encoder::new(writer, 3)?.auto_finish();
        self.write(&mut writer)?;
        writer.flush()?;
        Ok(())
    }

    /// Write the collection of BlockRange to an output handle
    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        self.ranges
            .iter()
            .try_for_each(|range| -> Result<()> { range.write_bytes(writer) })
    }

    fn add_range(&mut self, range: BlockRange) {
        self.ranges.push(range);
    }

    /// Builds an index from a VBQ file
    pub fn from_vbq<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };

        // Read header from mapped memory (unused but checks for validity)
        let _header = {
            let mut header_bytes = [0u8; SIZE_HEADER];
            header_bytes.copy_from_slice(&mmap[..SIZE_HEADER]);
            VBinseqHeader::from_bytes(&header_bytes)?
        };

        // Initialize position after the header
        let mut pos = SIZE_HEADER;

        // Initialize the collection
        let mut index = BlockIndex::default();

        // Find all block headers
        while pos < mmap.len() {
            let block_header = {
                let mut header_bytes = [0u8; SIZE_BLOCK_HEADER];
                header_bytes.copy_from_slice(&mmap[pos..pos + SIZE_BLOCK_HEADER]);
                BlockHeader::from_bytes(&header_bytes)?
            };
            index.add_range(BlockRange::new(pos as u64, block_header.size));
            pos += SIZE_BLOCK_HEADER + block_header.size as usize;
        }

        Ok(index)
    }

    /// Reads an index from a path
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let buffer = {
            let mut buffer = Vec::new();
            let mut handle = File::open(path).map(BufReader::new).map(Decoder::new)??;
            handle.read_to_end(&mut buffer)?;
            buffer
        };

        let mut ranges = Self::default();
        let mut pos = 0;
        while pos < buffer.len() {
            let bound = pos + SIZE_BLOCK_RANGE;
            let range = BlockRange::from_bytes(&buffer[pos..bound]);
            ranges.add_range(range);
            pos += SIZE_BLOCK_RANGE;
        }

        Ok(ranges)
    }

    /// Get a reference to the internal ranges
    pub fn ranges(&self) -> &[BlockRange] {
        &self.ranges
    }

    pub fn pprint(&self) {
        self.ranges.iter().for_each(|range| {
            println!("{}\t{}", range.start_offset, range.len);
        })
    }
}
