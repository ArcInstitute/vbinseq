use std::io::{Read, Write};

use byteorder::{ByteOrder, LittleEndian};

use crate::error::{HeaderError, Result};

/// Current magic number: "VSEQ" in ASCII
const MAGIC: u32 = 0x56534551;

/// Current format version
const FORMAT: u8 = 1;

/// Size of the header in bytes
pub const SIZE_HEADER: usize = 32;

/// Default block size: 1MB
pub const BLOCK_SIZE: u64 = 1024 * 1024;

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

    /// Reserved remaining bytes for future use
    ///
    /// 19 bytes
    pub reserved: [u8; 19],
}
impl Default for VBinseqHeader {
    fn default() -> Self {
        Self::new(BLOCK_SIZE)
    }
}
impl VBinseqHeader {
    pub fn new(block_size: u64) -> Self {
        Self {
            magic: MAGIC,
            format: FORMAT,
            block: block_size,
            reserved: [42; 19],
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
        let reserved = match buffer[13..32].try_into() {
            Ok(reserved) => reserved,
            Err(_) => return Err(HeaderError::InvalidReservedBytes.into()),
        };
        Ok(Self {
            magic,
            format,
            block,
            reserved,
        })
    }

    pub fn write_bytes<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut buffer = [0u8; SIZE_HEADER];
        LittleEndian::write_u32(&mut buffer[0..4], self.magic);
        buffer[4] = self.format;
        LittleEndian::write_u64(&mut buffer[5..13], self.block);
        buffer[13..32].copy_from_slice(&self.reserved);
        writer.write_all(&buffer)?;
        Ok(())
    }

    pub fn from_reader<R: Read>(reader: &mut R) -> Result<Self> {
        let mut buffer = [0u8; SIZE_HEADER];
        reader.read_exact(&mut buffer)?;
        Self::from_bytes(&buffer)
    }
}

pub struct BlockHeader {}
