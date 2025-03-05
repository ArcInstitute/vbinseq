use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

use byteorder::{ByteOrder, LittleEndian};
use zstd::{Decoder, Encoder};

use crate::{
    error::IndexError,
    header::{SIZE_BLOCK_HEADER, SIZE_HEADER},
    BlockHeader, Result, VBinseqHeader,
};

/// Size of BlockRange in bytes
pub const SIZE_BLOCK_RANGE: usize = 32;
/// Size of IndexHeader in bytes
pub const INDEX_HEADER_SIZE: usize = 32;
/// Magic number to designate index (VBQINDEX)
pub const INDEX_MAGIC: u64 = 0x5845444e49514256;
/// Index Block Reservation
pub const INDEX_RESERVATION: [u8; 8] = [42; 8];

/// Descriptor of the dimensions of a Block
#[derive(Debug, Clone, Copy)]
pub struct BlockRange {
    /// File offset where the block starts (including block header + file header)
    ///
    /// (8 bytes)
    pub start_offset: u64,
    /// Actual size of the block
    ///
    /// (8 bytes)
    pub len: u64,
    /// Number of records in block
    ///
    /// (4 bytes)
    pub block_records: u32,
    /// Cumulative number of records
    ///
    /// (4 bytes)
    pub cumulative_records: u32,
    /// Future extension reservation
    ///
    /// (8 bytes)
    pub reservation: [u8; 8],
}
impl BlockRange {
    pub fn new(start_offset: u64, len: u64, block_records: u32, cumulative_records: u32) -> Self {
        Self {
            start_offset,
            len,
            block_records,
            cumulative_records,
            reservation: INDEX_RESERVATION,
        }
    }

    /// Write self into a write handle
    pub fn write_bytes<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut buf = [0; SIZE_BLOCK_RANGE];
        LittleEndian::write_u64(&mut buf[0..8], self.start_offset);
        LittleEndian::write_u64(&mut buf[8..16], self.len);
        LittleEndian::write_u32(&mut buf[16..20], self.block_records);
        LittleEndian::write_u32(&mut buf[20..24], self.cumulative_records);
        buf[24..].copy_from_slice(&self.reservation);
        writer.write_all(&buf)?;
        Ok(())
    }

    /// Read self from exact byte buffer
    pub fn from_exact(buffer: &[u8; SIZE_BLOCK_RANGE]) -> Self {
        Self {
            start_offset: LittleEndian::read_u64(&buffer[0..8]),
            len: LittleEndian::read_u64(&buffer[8..16]),
            block_records: LittleEndian::read_u32(&buffer[16..20]),
            cumulative_records: LittleEndian::read_u32(&buffer[20..24]),
            reservation: INDEX_RESERVATION,
        }
    }

    /// Read self from byte buffer
    pub fn from_bytes(buffer: &[u8]) -> Self {
        let mut buf = [0; SIZE_BLOCK_RANGE];
        buf.copy_from_slice(buffer);
        Self::from_exact(&buf)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct IndexHeader {
    /// Magic number to designate the index
    ///
    /// (8 bytes)
    magic: u64,
    /// Number of bytes in the file (quickcheck that file/index are matched)
    ///
    /// (8 bytes)
    bytes: u64,
    /// Reserved bytes
    reserved: [u8; INDEX_HEADER_SIZE - 16],
}
impl IndexHeader {
    pub fn new(bytes: u64) -> Self {
        Self {
            magic: INDEX_MAGIC,
            bytes,
            reserved: [42; INDEX_HEADER_SIZE - 16],
        }
    }
    pub fn from_reader<R: Read>(reader: &mut R) -> Result<Self> {
        let mut buffer = [0; INDEX_HEADER_SIZE];
        reader.read_exact(&mut buffer)?;
        let magic = LittleEndian::read_u64(&buffer[0..8]);
        let bytes = LittleEndian::read_u64(&buffer[8..16]);
        let _reserved = &buffer[16..INDEX_HEADER_SIZE]; // Not used but bytes pulled to validate size
        if magic != INDEX_MAGIC {
            return Err(IndexError::InvalidMagicNumber(magic).into());
        }
        Ok(Self {
            magic,
            bytes,
            reserved: [42; INDEX_HEADER_SIZE - 16],
        })
    }
    pub fn write_bytes<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut buffer = [0; INDEX_HEADER_SIZE];
        LittleEndian::write_u64(&mut buffer[0..8], self.magic);
        LittleEndian::write_u64(&mut buffer[8..16], self.bytes);
        buffer[16..].copy_from_slice(&self.reserved);
        writer.write_all(&buffer)?;
        Ok(())
    }
}

/// Collection of block ranges forming an index
#[derive(Debug, Clone)]
pub struct BlockIndex {
    header: IndexHeader,
    ranges: Vec<BlockRange>,
}
impl BlockIndex {
    pub fn new(header: IndexHeader) -> Self {
        Self {
            header,
            ranges: Vec::default(),
        }
    }
    pub fn n_blocks(&self) -> usize {
        self.ranges.len()
    }

    /// Writes the collection of BlockRange to a file
    pub fn save_to_path<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let mut writer = File::create(path).map(BufWriter::new)?;
        self.header.write_bytes(&mut writer)?;
        let mut writer = Encoder::new(writer, 3)?.auto_finish();
        self.write_range(&mut writer)?;
        writer.flush()?;
        Ok(())
    }

    /// Write the collection of BlockRange to an output handle
    pub fn write_range<W: Write>(&self, writer: &mut W) -> Result<()> {
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
        let file_size = mmap.len();

        // Read header from mapped memory (unused but checks for validity)
        let _header = {
            let mut header_bytes = [0u8; SIZE_HEADER];
            header_bytes.copy_from_slice(&mmap[..SIZE_HEADER]);
            VBinseqHeader::from_bytes(&header_bytes)?
        };

        // Initialize position after the header
        let mut pos = SIZE_HEADER;

        // Initialize the collection
        let index_header = IndexHeader::new(file_size as u64);
        let mut index = BlockIndex::new(index_header);

        // Find all block headers
        let mut record_total = 0;
        while pos < mmap.len() {
            let block_header = {
                let mut header_bytes = [0u8; SIZE_BLOCK_HEADER];
                header_bytes.copy_from_slice(&mmap[pos..pos + SIZE_BLOCK_HEADER]);
                BlockHeader::from_bytes(&header_bytes)?
            };
            index.add_range(BlockRange::new(
                pos as u64,
                block_header.size,
                block_header.records,
                record_total,
            ));
            pos += SIZE_BLOCK_HEADER + block_header.size as usize;
            record_total += block_header.records;
        }

        Ok(index)
    }

    /// Reads an index from a path
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let upstream_file =
            if let Some(upstream) = path.as_ref().to_str().unwrap().strip_suffix(".vqi") {
                upstream
            } else {
                return Err(IndexError::MissingUpstreamFile(
                    path.as_ref().to_string_lossy().to_string(),
                )
                .into());
            };
        let upstream_handle = File::open(upstream_file)?;
        let mmap = unsafe { memmap2::Mmap::map(&upstream_handle)? };
        let file_size = mmap.len() as u64;

        let mut file_handle = File::open(path).map(BufReader::new)?;
        let index_header = IndexHeader::from_reader(&mut file_handle)?;
        if index_header.bytes != file_size {
            return Err(IndexError::ByteSizeMismatch(file_size, index_header.bytes).into());
        }
        let buffer = {
            let mut buffer = Vec::new();
            let mut decoder = Decoder::new(file_handle)?;
            decoder.read_to_end(&mut buffer)?;
            buffer
        };

        let mut ranges = Self::new(index_header);
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
            println!(
                "{}\t{}\t{}\t{}",
                range.start_offset, range.len, range.block_records, range.cumulative_records
            );
        })
    }
}
