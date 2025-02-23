use std::io::{Read, Write};

use byteorder::{ByteOrder, LittleEndian};

use crate::error::{HeaderError, ReadError, Result};

/// Current magic number: "VSEQ" in ASCII
const MAGIC: u32 = 0x56534551;

/// Current magic number: "BLOCKSEQ"
const BLOCK_MAGIC: u64 = 0x424C4F434B534551;

/// Current format version
const FORMAT: u8 = 1;

/// Size of the header in bytes
pub const SIZE_HEADER: usize = 32;

/// Size of the block header in bytes
pub const SIZE_BLOCK_HEADER: usize = 8;

/// Default block size: 64KB
pub const BLOCK_SIZE: u64 = 128 * 1024;

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

    /// Reserved remaining bytes for future use
    ///
    /// 18 bytes
    pub reserved: [u8; 18],
}
impl Default for VBinseqHeader {
    fn default() -> Self {
        Self::with_capacity(BLOCK_SIZE, false)
    }
}
impl VBinseqHeader {
    pub fn new(qual: bool) -> Self {
        Self::with_capacity(BLOCK_SIZE, qual)
    }

    pub fn with_capacity(block: u64, qual: bool) -> Self {
        Self {
            magic: MAGIC,
            format: FORMAT,
            block,
            qual,
            reserved: [42; 18],
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
        let reserved = match buffer[14..32].try_into() {
            Ok(reserved) => reserved,
            Err(_) => return Err(HeaderError::InvalidReservedBytes.into()),
        };
        Ok(Self {
            magic,
            format,
            block,
            qual,
            reserved,
        })
    }

    pub fn write_bytes<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut buffer = [0u8; SIZE_HEADER];
        LittleEndian::write_u32(&mut buffer[0..4], self.magic);
        buffer[4] = self.format;
        LittleEndian::write_u64(&mut buffer[5..13], self.block);
        buffer[13] = if self.qual { 1 } else { 0 };
        buffer[14..32].copy_from_slice(&self.reserved);
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
    pub magic: u64,
}
impl Default for BlockHeader {
    fn default() -> Self {
        Self::new()
    }
}
impl BlockHeader {
    pub fn new() -> Self {
        Self { magic: BLOCK_MAGIC }
    }

    pub fn write_bytes<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut buffer = [0u8; 8];
        LittleEndian::write_u64(&mut buffer, self.magic);
        writer.write_all(&buffer)?;
        Ok(())
    }

    pub fn validate(buffer: &[u8], pos: usize) -> Result<()> {
        let magic = LittleEndian::read_u64(&buffer[pos..pos + 8]);
        if magic != BLOCK_MAGIC {
            return Err(ReadError::InvalidBlockMagicNumber(magic, pos).into());
        }
        Ok(())
    }
}
