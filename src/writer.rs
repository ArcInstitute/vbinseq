use std::io::Write;

use byteorder::{LittleEndian, WriteBytesExt};
use zstd::Encoder;

use crate::error::{Result, WriteError};
use crate::header::{BlockHeader, VBinseqHeader};

/// The record byte size is the size of the embedded buffer in bytes
/// as well as the size of the flag and length of the buffer.
///
/// S = w(Cs + Cx + 3)
///
/// Where:
/// - w: word size (8 bytes)
/// - Cs: Chunk size (primary sequence)
/// - Cx: Chunk size (extended sequence)
/// - 3: flag + slen + xlen
pub fn record_byte_size(schunk: usize, xchunk: usize) -> usize {
    8 * (schunk + xchunk + 3)
}

/// The record byte size is the size of the embedded buffer in bytes
/// plus the preamble (flag + slen + xlen)
///
/// This also includes the quality score length which is 1 byte per base.
pub fn record_byte_size_quality(schunk: usize, xchunk: usize, slen: usize, xlen: usize) -> usize {
    record_byte_size(schunk, xchunk) + slen + xlen
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
///    block header (at the appropriate position) and start a new block.
/// 2. Write the `Record` to the current block.
pub struct VBinseqWriter<W: Write> {
    /// Inner Writer
    inner: W,

    /// Header of the file
    header: VBinseqHeader,

    /// Reusable buffer for all primary nucleotides (written as 2-bit after conversion)
    sbuffer: Vec<u64>,

    /// Reusable buffer for all extended nucleotides (written as 2-bit after conversion)
    xbuffer: Vec<u64>,

    /// Pre-initialized writer for compressed blocks
    cblock: BlockWriter,
}
impl<W: Write> VBinseqWriter<W> {
    pub fn new(inner: W, header: VBinseqHeader) -> Result<Self> {
        let mut wtr = Self {
            inner,
            header,
            sbuffer: Vec::new(),
            xbuffer: Vec::new(),
            cblock: BlockWriter::new(header.block as usize, header.compressed),
        };
        wtr.init()?;
        Ok(wtr)
    }

    /// Initialize the writer by writing the header and the first block header.
    fn init(&mut self) -> Result<()> {
        self.header.write_bytes(&mut self.inner)?;
        Ok(())
    }

    pub fn write_nucleotides(&mut self, flag: u64, sequence: &[u8]) -> Result<bool> {
        // Validate the right write operation is being used
        if self.header.qual {
            return Err(WriteError::QualityFlagSet.into());
        }
        if self.header.paired {
            return Err(WriteError::PairedFlagSet.into());
        }

        // encode the sequence
        self.sbuffer.clear();
        if bitnuc::encode(sequence, &mut self.sbuffer).is_err() {
            return Ok(false);
        }

        // Check if the current block can handle the next record
        let record_size = record_byte_size(self.sbuffer.len(), 0);
        if self.cblock.exceeds_block_size(record_size)? {
            self.cblock.flush(&mut self.inner)?;
        }

        // Write the flag, length, and sequence to the block
        self.cblock.write_flag(flag)?;
        self.cblock.write_length(sequence.len() as u64)?;
        self.cblock.write_length(0)?;
        self.cblock.write_buffer(&self.sbuffer)?;

        Ok(true)
    }

    pub fn write_nucleotides_paired(
        &mut self,
        flag: u64,
        primary: &[u8],
        extended: &[u8],
    ) -> Result<bool> {
        // Validate the right write operation is being used
        if self.header.qual {
            return Err(WriteError::QualityFlagSet.into());
        }
        if !self.header.paired {
            return Err(WriteError::PairedFlagNotSet.into());
        }

        // encode the sequence
        self.sbuffer.clear();
        if bitnuc::encode(primary, &mut self.sbuffer).is_err() {
            return Ok(false);
        }

        self.xbuffer.clear();
        if bitnuc::encode(extended, &mut self.xbuffer).is_err() {
            return Ok(false);
        }

        // Check if the current block can handle the next record
        let record_size = record_byte_size(self.sbuffer.len(), self.xbuffer.len());
        if self.cblock.exceeds_block_size(record_size)? {
            self.cblock.flush(&mut self.inner)?;
        }

        // Write the flag, length, and sequence to the block
        self.cblock.write_flag(flag)?;
        self.cblock.write_length(primary.len() as u64)?;
        self.cblock.write_length(extended.len() as u64)?;
        self.cblock.write_buffer(&self.sbuffer)?;
        self.cblock.write_buffer(&self.xbuffer)?;

        Ok(true)
    }

    /// Writes nucleotides and quality scores to the writer.
    pub fn write_nucleotides_quality(
        &mut self,
        flag: u64,
        sequence: &[u8],
        quality: &[u8],
    ) -> Result<bool> {
        // Validate the right write operation is being used
        if !self.header.qual {
            return Err(WriteError::QualityFlagNotSet.into());
        }
        if self.header.paired {
            return Err(WriteError::PairedFlagSet.into());
        }

        // encode the sequence
        self.sbuffer.clear();
        if bitnuc::encode(sequence, &mut self.sbuffer).is_err() {
            return Ok(false);
        }

        // Check if the current block can handle the next record
        let record_size = record_byte_size_quality(self.sbuffer.len(), 0, quality.len(), 0);
        if self.cblock.exceeds_block_size(record_size)? {
            self.cblock.flush(&mut self.inner)?;
        }

        // Write the flag, length, sequence, and quality scores to the block
        self.cblock.write_flag(flag)?;
        self.cblock.write_length(sequence.len() as u64)?;
        self.cblock.write_length(0)?;
        self.cblock.write_buffer(&self.sbuffer)?;
        self.cblock.write_quality(quality)?;

        Ok(true)
    }

    /// Writes paired nucleotides and quality scores to the writer.
    pub fn write_nucleotides_quality_paired(
        &mut self,
        flag: u64,
        s_seq: &[u8],
        x_seq: &[u8],
        s_qual: &[u8],
        x_qual: &[u8],
    ) -> Result<bool> {
        // Validate the right write operation is being used
        if !self.header.qual {
            return Err(WriteError::QualityFlagNotSet.into());
        }
        if !self.header.paired {
            return Err(WriteError::PairedFlagNotSet.into());
        }

        // encode the sequence
        self.sbuffer.clear();
        if bitnuc::encode(s_seq, &mut self.sbuffer).is_err() {
            return Ok(false);
        }
        self.xbuffer.clear();
        if bitnuc::encode(x_seq, &mut self.xbuffer).is_err() {
            return Ok(false);
        }

        // Check if the current block can handle the next record
        let record_size = record_byte_size_quality(
            self.sbuffer.len(),
            self.xbuffer.len(),
            s_qual.len(),
            x_qual.len(),
        );
        if self.cblock.exceeds_block_size(record_size)? {
            self.cblock.flush(&mut self.inner)?;
        }

        // Write the flag, length, sequence, and quality scores to the block
        self.cblock.write_flag(flag)?;
        self.cblock.write_length(s_seq.len() as u64)?;
        self.cblock.write_length(x_seq.len() as u64)?;
        self.cblock.write_buffer(&self.sbuffer)?;
        self.cblock.write_quality(s_qual)?;
        self.cblock.write_buffer(&self.xbuffer)?;
        self.cblock.write_quality(x_qual)?;

        Ok(true)
    }

    /// Finishes the internal writer.
    pub fn finish(&mut self) -> Result<()> {
        self.cblock.flush(&mut self.inner)?;
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

struct BlockWriter {
    /// Current position in the block
    pos: usize,
    /// Virtual block size
    block_size: usize,
    /// Compression level
    level: i32,
    /// Uncompressed buffer
    ubuf: Vec<u8>,
    /// Compressed buffer
    zbuf: Vec<u8>,
    /// Reusable padding buffer
    padding: Vec<u8>,
    /// Compression flag
    /// If false, the block is written uncompressed
    compress: bool,
}
impl BlockWriter {
    fn new(block_size: usize, compress: bool) -> Self {
        Self {
            pos: 0,
            block_size,
            level: 3,
            ubuf: Vec::with_capacity(block_size),
            zbuf: Vec::with_capacity(block_size),
            padding: vec![0; block_size],
            compress,
        }
    }

    fn exceeds_block_size(&self, record_size: usize) -> Result<bool> {
        if record_size > self.block_size {
            return Err(WriteError::RecordSizeExceedsMaximumBlockSize(
                record_size,
                self.block_size,
            )
            .into());
        }
        Ok(self.pos + record_size > self.block_size)
    }

    fn write_flag(&mut self, flag: u64) -> Result<()> {
        self.ubuf.write_u64::<LittleEndian>(flag)?;
        self.pos += 8;
        Ok(())
    }

    fn write_length(&mut self, length: u64) -> Result<()> {
        self.ubuf.write_u64::<LittleEndian>(length)?;
        self.pos += 8;
        Ok(())
    }

    fn write_buffer(&mut self, ebuf: &[u64]) -> Result<()> {
        ebuf.iter()
            .try_for_each(|&x| self.ubuf.write_u64::<LittleEndian>(x))?;
        self.pos += 8 * ebuf.len();
        Ok(())
    }

    fn write_quality(&mut self, quality: &[u8]) -> Result<()> {
        self.ubuf.write_all(quality)?;
        self.pos += quality.len();
        Ok(())
    }

    fn flush_compressed<W: Write>(&mut self, inner: &mut W) -> Result<()> {
        // Encode the block
        let mut encoder = Encoder::new(&mut self.zbuf, self.level)?;
        encoder.write_all(&self.ubuf)?;
        encoder.finish()?;

        // Build a block header (this is variably sized in the compressed case)
        let header = BlockHeader::new(self.zbuf.len() as u64);

        // Write the block header and compressed block
        header.write_bytes(inner)?;
        inner.write_all(&self.zbuf)?;

        Ok(())
    }

    fn flush_uncompressed<W: Write>(&mut self, inner: &mut W) -> Result<()> {
        // Build a block header (this is static in size in the uncompressed case)
        let header = BlockHeader::new(self.block_size as u64);

        // Write the block header and uncompressed block
        header.write_bytes(inner)?;
        inner.write_all(&self.ubuf)?;

        Ok(())
    }

    fn flush<W: Write>(&mut self, inner: &mut W) -> Result<()> {
        // Skip if the block is empty
        if self.pos == 0 {
            return Ok(());
        }

        // Finish out the block with padding
        let bytes_to_next_start = self.block_size - self.pos;
        self.ubuf.write_all(&self.padding[..bytes_to_next_start])?;

        // Flush the block (implemented differently based on compression)
        if self.compress {
            self.flush_compressed(inner)?;
        } else {
            self.flush_uncompressed(inner)?;
        }

        // Reset the position and buffers
        self.clear();

        Ok(())
    }

    fn clear(&mut self) {
        self.pos = 0;
        self.ubuf.clear();
        self.zbuf.clear();
    }
}
