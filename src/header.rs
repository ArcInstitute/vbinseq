use std::io::{Read, Write};

use byteorder::{ByteOrder, LittleEndian};

use crate::error::{HeaderError, ReadError, Result};

/// Current magic number: "VSEQ" in ASCII
const MAGIC: u32 = 0x51455356;

/// Current magic number: "BLOCKSEQ"
const BLOCK_MAGIC: u64 = 0x5145534B434F4C42;

/// Current format version
const FORMAT: u8 = 1;

/// Size of the header in bytes
pub const SIZE_HEADER: usize = 32;

/// Size of the block header in bytes
pub const SIZE_BLOCK_HEADER: usize = 32;

/// Default block size: 64KB
pub const BLOCK_SIZE: u64 = 128 * 1024;

/// Reserved bytes for future use (File Header)
pub const RESERVED_BYTES: [u8; 16] = [42; 16];

/// Reserved bytes for future use (Block Header)
pub const RESERVED_BYTES_BLOCK: [u8; 12] = [42; 12];

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VBinseqHeader {
    /// Magic number to identify the file format
    ///
    /// 4 bytes
    pub magic: u32,

    /// Version of the file format
    ///
    /// 1 byte
    pub format: u8,

    /// Block size in bytes
    ///
    /// 8 byte
    pub block: u64,

    /// Quality scores included
    ///
    /// 1 byte
    pub qual: bool,

    /// Internal blocks are zstd compressed
    ///
    /// 1 byte
    pub compressed: bool,

    /// Records are paired
    ///
    /// 1 byte
    pub paired: bool,

    /// Reserved remaining bytes for future use
    ///
    /// 16 bytes
    pub reserved: [u8; 16],
}
impl Default for VBinseqHeader {
    fn default() -> Self {
        Self::with_capacity(BLOCK_SIZE, false, false, false)
    }
}
impl VBinseqHeader {
    pub fn new(qual: bool, compressed: bool, paired: bool) -> Self {
        Self::with_capacity(BLOCK_SIZE, qual, compressed, paired)
    }

    pub fn with_capacity(block: u64, qual: bool, compressed: bool, paired: bool) -> Self {
        Self {
            magic: MAGIC,
            format: FORMAT,
            block,
            qual,
            compressed,
            paired,
            reserved: RESERVED_BYTES,
        }
    }

    pub fn from_bytes(buffer: &[u8; SIZE_HEADER]) -> Result<Self> {
        let magic = LittleEndian::read_u32(&buffer[0..4]);
        if magic != MAGIC {
            return Err(HeaderError::InvalidMagicNumber(magic).into());
        }
        let format = buffer[4];
        if format != FORMAT {
            return Err(HeaderError::InvalidFormatVersion(format).into());
        }
        let block = LittleEndian::read_u64(&buffer[5..13]);
        let qual = buffer[13] != 0;
        let compressed = buffer[14] != 0;
        let paired = buffer[15] != 0;
        let reserved = match buffer[16..32].try_into() {
            Ok(reserved) => reserved,
            Err(_) => return Err(HeaderError::InvalidReservedBytes.into()),
        };
        Ok(Self {
            magic,
            format,
            block,
            qual,
            compressed,
            reserved,
            paired,
        })
    }

    pub fn write_bytes<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut buffer = [0u8; SIZE_HEADER];
        LittleEndian::write_u32(&mut buffer[0..4], self.magic);
        buffer[4] = self.format;
        LittleEndian::write_u64(&mut buffer[5..13], self.block);
        buffer[13] = if self.qual { 1 } else { 0 };
        buffer[14] = if self.compressed { 1 } else { 0 };
        buffer[15] = if self.compressed { 1 } else { 0 };
        buffer[16..32].copy_from_slice(&self.reserved);
        writer.write_all(&buffer)?;
        Ok(())
    }

    pub fn from_reader<R: Read>(reader: &mut R) -> Result<Self> {
        let mut buffer = [0u8; SIZE_HEADER];
        reader.read_exact(&mut buffer)?;
        Self::from_bytes(&buffer)
    }
}

#[derive(Clone, Copy)]
pub struct BlockHeader {
    /// Magic number to identify the block format
    ///
    /// (8 bytes)
    pub magic: u64,

    /// Actual size of the block in bytes
    ///
    /// Can vary from the block size in the header
    /// depending on compression status
    ///
    /// (8 bytes)
    pub size: u64,

    /// Number of records in this block
    ///
    /// (4 bytes)
    pub records: u32,

    /// Reserved bytes in case of future extension
    ///
    /// (8 bytes)
    pub reserved: [u8; 12],
}
impl BlockHeader {
    pub fn new(size: u64, records: u32) -> Self {
        Self {
            magic: BLOCK_MAGIC,
            size,
            records,
            reserved: RESERVED_BYTES_BLOCK,
        }
    }

    pub fn write_bytes<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut buffer = [0u8; SIZE_BLOCK_HEADER];
        LittleEndian::write_u64(&mut buffer[0..8], self.magic);
        LittleEndian::write_u64(&mut buffer[8..16], self.size);
        LittleEndian::write_u32(&mut buffer[16..20], self.records);
        buffer[20..].copy_from_slice(&self.reserved);
        writer.write_all(&buffer)?;
        Ok(())
    }

    pub fn from_bytes(buffer: &[u8; SIZE_BLOCK_HEADER]) -> Result<Self> {
        let magic = LittleEndian::read_u64(&buffer[0..8]);
        if magic != BLOCK_MAGIC {
            return Err(ReadError::InvalidBlockMagicNumber(magic, 0).into());
        }
        let size = LittleEndian::read_u64(&buffer[8..16]);
        let records = LittleEndian::read_u32(&buffer[16..20]);
        Ok(Self::new(size, records))
    }
}
