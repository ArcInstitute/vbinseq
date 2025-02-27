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
pub const SIZE_BLOCK_HEADER: usize = 16;

/// Default block size: 64KB
pub const BLOCK_SIZE: u64 = 128 * 1024;

/// Reserved bytes for future use
pub const RESERVED_BYTES: [u8; 17] = [42; 17];

#[derive(Clone, Copy)]
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

    /// Reserved remaining bytes for future use
    ///
    /// 17 bytes
    pub reserved: [u8; 17],
}
impl Default for VBinseqHeader {
    fn default() -> Self {
        Self::with_capacity(BLOCK_SIZE, false, false)
    }
}
impl VBinseqHeader {
    pub fn new(qual: bool, compressed: bool) -> Self {
        Self::with_capacity(BLOCK_SIZE, qual, compressed)
    }

    pub fn with_capacity(block: u64, qual: bool, compressed: bool) -> Self {
        Self {
            magic: MAGIC,
            format: FORMAT,
            block,
            qual,
            compressed,
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
        let reserved = match buffer[15..32].try_into() {
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
        })
    }

    pub fn write_bytes<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut buffer = [0u8; SIZE_HEADER];
        LittleEndian::write_u32(&mut buffer[0..4], self.magic);
        buffer[4] = self.format;
        LittleEndian::write_u64(&mut buffer[5..13], self.block);
        buffer[13] = if self.qual { 1 } else { 0 };
        buffer[14] = if self.compressed { 1 } else { 0 };
        buffer[15..32].copy_from_slice(&self.reserved);
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
    pub magic: u64,

    /// Actual size of the block in bytes
    ///
    /// Can vary from the block size in the header
    /// depending on compression status
    pub size: u64,
}
impl BlockHeader {
    pub fn new(size: u64) -> Self {
        Self {
            magic: BLOCK_MAGIC,
            size,
        }
    }

    pub fn write_bytes<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut buffer = [0u8; 16];
        LittleEndian::write_u64(&mut buffer[0..8], self.magic);
        LittleEndian::write_u64(&mut buffer[8..16], self.size);
        writer.write_all(&buffer)?;
        Ok(())
    }

    pub fn from_bytes(buffer: &[u8; SIZE_BLOCK_HEADER]) -> Result<Self> {
        let magic = LittleEndian::read_u64(&buffer[0..8]);
        if magic != BLOCK_MAGIC {
            return Err(ReadError::InvalidBlockMagicNumber(magic, 0).into());
        }
        let size = LittleEndian::read_u64(&buffer[8..16]);
        Ok(Self { magic, size })
    }
}
