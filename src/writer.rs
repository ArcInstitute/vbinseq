use std::io::Write;

use byteorder::{LittleEndian, WriteBytesExt};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use zstd::Encoder as ZstdEncoder;

use crate::error::{Result, WriteError};
use crate::header::{BlockHeader, VBinseqHeader};
use crate::Policy;

pub const RNG_SEED: u64 = 42;

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

/// A builder for the VBinseqWriter
#[derive(Default)]
pub struct VBinseqWriterBuilder {
    /// Header of the file
    header: Option<VBinseqHeader>,
    /// Optional policy for encoding
    policy: Option<Policy>,
    /// Optional headless mode (used in parallel writing)
    headless: Option<bool>,
}
impl VBinseqWriterBuilder {
    pub fn header(mut self, header: VBinseqHeader) -> Self {
        self.header = Some(header);
        self
    }

    pub fn policy(mut self, policy: Policy) -> Self {
        self.policy = Some(policy);
        self
    }

    pub fn headless(mut self, headless: bool) -> Self {
        self.headless = Some(headless);
        self
    }

    pub fn build<W: Write>(self, inner: W) -> Result<VBinseqWriter<W>> {
        let Some(header) = self.header else {
            return Err(WriteError::MissingHeader.into());
        };
        VBinseqWriter::new(
            inner,
            header,
            self.policy.unwrap_or_default(),
            self.headless.unwrap_or(false),
        )
    }
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

    /// Encoder for nucleotide sequences
    encoder: Encoder,

    /// Pre-initialized writer for compressed blocks
    cblock: BlockWriter,
}
impl<W: Write> VBinseqWriter<W> {
    pub fn new(inner: W, header: VBinseqHeader, policy: Policy, headless: bool) -> Result<Self> {
        let mut wtr = Self {
            inner,
            header,
            encoder: Encoder::with_policy(policy),
            cblock: BlockWriter::new(header.block as usize, header.compressed),
        };
        if !headless {
            wtr.init()?;
        }
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
        if let Some(sbuffer) = self.encoder.encode_single(sequence)? {
            let record_size = record_byte_size(sbuffer.len(), 0);
            if self.cblock.exceeds_block_size(record_size)? {
                self.cblock.flush(&mut self.inner)?;
            }

            // Write the flag, length, and sequence to the block
            self.cblock.write_flag(flag)?;
            self.cblock.write_length(sequence.len() as u64)?;
            self.cblock.write_length(0)?;
            self.cblock.write_buffer(sbuffer)?;

            // Return true if the sequence was successfully written
            Ok(true)
        } else {
            // Silently ignore sequences that fail encoding
            Ok(false)
        }
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

        if let Some((sbuffer, xbuffer)) = self.encoder.encode_paired(primary, extended)? {
            // Check if the current block can handle the next record
            let record_size = record_byte_size(sbuffer.len(), xbuffer.len());
            if self.cblock.exceeds_block_size(record_size)? {
                self.cblock.flush(&mut self.inner)?;
            }

            // Write the flag, length, and sequence to the block
            self.cblock.write_flag(flag)?;
            self.cblock.write_length(primary.len() as u64)?;
            self.cblock.write_length(extended.len() as u64)?;
            self.cblock.write_buffer(sbuffer)?;
            self.cblock.write_buffer(xbuffer)?;

            // Return true if the record was successfully written
            Ok(true)
        } else {
            // Return false if the record was not successfully written
            Ok(false)
        }
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

        if let Some(sbuffer) = self.encoder.encode_single(sequence)? {
            // Check if the current block can handle the next record
            let record_size = record_byte_size_quality(sbuffer.len(), 0, quality.len(), 0);
            if self.cblock.exceeds_block_size(record_size)? {
                self.cblock.flush(&mut self.inner)?;
            }

            // Write the flag, length, sequence, and quality scores to the block
            self.cblock.write_flag(flag)?;
            self.cblock.write_length(sequence.len() as u64)?;
            self.cblock.write_length(0)?;
            self.cblock.write_buffer(sbuffer)?;
            self.cblock.write_quality(quality)?;

            // Return true if the record was written successfully
            Ok(true)
        } else {
            // Return false if the record was not written successfully
            Ok(false)
        }
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

        if let Some((sbuffer, xbuffer)) = self.encoder.encode_paired(s_seq, x_seq)? {
            // Check if the current block can handle the next record
            let record_size =
                record_byte_size_quality(sbuffer.len(), xbuffer.len(), s_qual.len(), x_qual.len());
            if self.cblock.exceeds_block_size(record_size)? {
                self.cblock.flush(&mut self.inner)?;
            }

            // Write the flag, length, sequence, and quality scores to the block
            self.cblock.write_flag(flag)?;
            self.cblock.write_length(s_seq.len() as u64)?;
            self.cblock.write_length(x_seq.len() as u64)?;
            self.cblock.write_buffer(sbuffer)?;
            self.cblock.write_quality(s_qual)?;
            self.cblock.write_buffer(xbuffer)?;
            self.cblock.write_quality(x_qual)?;

            // Return true if the record was successfully written
            Ok(true)
        } else {
            // Return false if the record was not successfully written
            Ok(false)
        }
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
        let mut encoder = ZstdEncoder::new(&mut self.zbuf, self.level)?;
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

/// Encapsulates the logic for encoding sequences into a binary format.
pub struct Encoder {
    /// Reusable buffers for all nucleotides (written as 2-bit after conversion)
    sbuffer: Vec<u64>,
    xbuffer: Vec<u64>,

    /// Reusable buffers for invalid nucleotide sequences
    s_ibuf: Vec<u8>,
    x_ibuf: Vec<u8>,

    /// Invalid Nucleotide Policy
    policy: Policy,

    /// Random Number Generator
    rng: SmallRng,
}

impl Default for Encoder {
    fn default() -> Self {
        Self::with_policy(Policy::default())
    }
}

impl Encoder {
    pub fn new() -> Self {
        Self::with_policy(Policy::default())
    }

    /// Initialize a new encoder with the given policy.
    pub fn with_policy(policy: Policy) -> Self {
        Self {
            policy,
            sbuffer: Vec::default(),
            xbuffer: Vec::default(),
            s_ibuf: Vec::default(),
            x_ibuf: Vec::default(),
            rng: SmallRng::seed_from_u64(RNG_SEED),
        }
    }

    /// Encodes a single sequence as 2-bit.
    ///
    /// Will return `None` if the sequence is invalid and the policy does not allow correction.
    pub fn encode_single(&mut self, primary: &[u8]) -> Result<Option<&[u64]>> {
        // Fill the buffer with the 2-bit representation of the nucleotides
        self.clear();
        if bitnuc::encode(primary, &mut self.sbuffer).is_err() {
            self.clear();
            if self
                .policy
                .handle(primary, &mut self.s_ibuf, &mut self.rng)?
            {
                bitnuc::encode(&self.s_ibuf, &mut self.sbuffer)?;
            } else {
                return Ok(None);
            }
        }
        Ok(Some(&self.sbuffer))
    }

    /// Encodes a pair of sequences as 2-bit.
    ///
    /// Will return `None` if either sequence is invalid and the policy does not allow correction.
    pub fn encode_paired(
        &mut self,
        primary: &[u8],
        extended: &[u8],
    ) -> Result<Option<(&[u64], &[u64])>> {
        self.clear();
        if bitnuc::encode(primary, &mut self.sbuffer).is_err()
            || bitnuc::encode(extended, &mut self.xbuffer).is_err()
        {
            self.clear();
            if self
                .policy
                .handle(primary, &mut self.s_ibuf, &mut self.rng)?
                && self
                    .policy
                    .handle(extended, &mut self.x_ibuf, &mut self.rng)?
            {
                bitnuc::encode(&self.s_ibuf, &mut self.sbuffer)?;
                bitnuc::encode(&self.x_ibuf, &mut self.xbuffer)?;
            } else {
                return Ok(None);
            }
        }
        Ok(Some((&self.sbuffer, &self.xbuffer)))
    }

    /// Clear all buffers and reset the encoder.
    pub fn clear(&mut self) {
        self.sbuffer.clear();
        self.xbuffer.clear();
        self.s_ibuf.clear();
        self.x_ibuf.clear();
    }
}
