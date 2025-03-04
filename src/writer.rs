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
            self.cblock
                .write_record(flag, sequence.len() as u64, 0, sbuffer, None, None, None)?;

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
            self.cblock.write_record(
                flag,
                primary.len() as u64,
                extended.len() as u64,
                sbuffer,
                None,
                Some(xbuffer),
                None,
            )?;

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
            self.cblock.write_record(
                flag,
                sequence.len() as u64,
                0,
                sbuffer,
                Some(quality),
                None,
                None,
            )?;

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
            self.cblock.write_record(
                flag,
                s_seq.len() as u64,
                x_seq.len() as u64,
                sbuffer,
                Some(s_qual),
                Some(xbuffer),
                Some(x_qual),
            )?;

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

    /// Provides a mutable reference to the inner writer
    fn by_ref(&mut self) -> &mut W {
        self.inner.by_ref()
    }

    /// Provides a mutable reference to the BlockWriter
    fn cblock_mut(&mut self) -> &mut BlockWriter {
        &mut self.cblock
    }

    /// Ingests the internal bytes of a VBinseqWriter whose inner writer is a Vec of bytes.
    ///
    /// Removes the bytes from the other writer after ingestion.
    pub fn ingest(&mut self, other: &mut VBinseqWriter<Vec<u8>>) -> Result<()> {
        // Write complete blocks from other directly
        // and clear the other (mimics reading)
        {
            self.inner.write_all(other.by_ref())?;
            other.by_ref().clear();
        }

        // Ingest incomplete block from other
        {
            self.cblock.ingest(other.cblock_mut(), &mut self.inner)?;
        }
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
    /// Tracks all record start positions in the block
    starts: Vec<usize>,
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
            starts: Vec::default(),
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

    #[allow(clippy::too_many_arguments)]
    fn write_record(
        &mut self,
        flag: u64,
        slen: u64,
        xlen: u64,
        sbuf: &[u64],
        squal: Option<&[u8]>,
        xbuf: Option<&[u64]>,
        xqual: Option<&[u8]>,
    ) -> Result<()> {
        // Tracks the record start position
        self.starts.push(self.pos);

        // Write the flag
        self.write_flag(flag)?;

        // Write the lengths
        self.write_length(slen)?;
        self.write_length(xlen)?;

        // Write the primary sequence and optional quality
        self.write_buffer(sbuf)?;
        if let Some(qual) = squal {
            self.write_quality(qual)?;
        }

        // Write the optional extended sequence and optional quality
        if let Some(xbuf) = xbuf {
            self.write_buffer(xbuf)?;
        }
        if let Some(qual) = xqual {
            self.write_quality(qual)?;
        }

        Ok(())
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
        self.starts.clear();
        self.ubuf.clear();
        self.zbuf.clear();
    }

    /// Ingests *all* bytes from another BlockWriter.
    ///
    /// Because both block sizes should be equivalent the process should take
    /// at most two steps.
    ///
    /// I.e. the bytes can either all fit directly into self.ubuf or an intermediate
    /// flush step is required.
    fn ingest<W: Write>(&mut self, other: &mut Self, inner: &mut W) -> Result<()> {
        // Number of available bytes in buffer (self)
        let remaining = self.block_size - self.pos;

        // Quick ingestion (take all without flush)
        if other.pos <= remaining {
            self.ingest_all(other)
        } else {
            self.ingest_subset(other)?;
            self.flush(inner)?;
            self.ingest_all(other)
        }
    }

    /// Takes all bytes from the other into self
    ///
    /// Do not call this directly - always go through `ingest`
    fn ingest_all(&mut self, other: &mut Self) -> Result<()> {
        let n_bytes = other.pos;

        // Drain bounded bytes from other (clearing them in the process)
        self.ubuf.write_all(other.ubuf.drain(..).as_slice())?;

        // Take starts from other (shifting them in the process)
        other
            .starts
            .drain(..)
            .for_each(|start| self.starts.push(start + self.pos));

        // Left shift all remaining starts in other
        other.starts.iter_mut().for_each(|x| {
            *x -= n_bytes;
        });

        // Shift position cursors
        self.pos += n_bytes;

        // Clear the other for good measure
        other.clear();

        Ok(())
    }

    /// Takes as many bytes as possible from the other into self
    ///
    /// Do not call this directly - always go through `ingest
    fn ingest_subset(&mut self, other: &mut Self) -> Result<()> {
        let remaining = self.block_size - self.pos;
        let (start_index, end_byte) = other
            .starts
            .iter()
            .enumerate()
            .take_while(|(_idx, x)| **x <= remaining)
            .last()
            .map(|(idx, x)| (idx, *x))
            .unwrap();

        // Drain bounded bytes from other (clearing them in the process)
        self.ubuf
            .write_all(other.ubuf.drain(0..end_byte).as_slice())?;

        // Take starts from other (shifting them in the process)
        other
            .starts
            .drain(0..start_index)
            .for_each(|start| self.starts.push(start + self.pos));

        // Left shift all remaining starts in other
        other.starts.iter_mut().for_each(|x| {
            *x -= end_byte;
        });

        // Shift position cursors
        self.pos += end_byte;
        other.pos -= end_byte;

        Ok(())
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

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn test_ingest_empty_writer() -> crate::Result<()> {
        // Test ingesting from an empty writer
        let header = VBinseqHeader::new(false, false, false);

        // Create a source writer that's empty
        let mut source = VBinseqWriterBuilder::default()
            .header(header)
            .headless(true)
            .build(Vec::new())?;

        // Create a destination writer
        let mut dest = VBinseqWriterBuilder::default()
            .header(header)
            .headless(true)
            .build(Vec::new())?;

        // Ingest from source to dest
        dest.ingest(&mut source)?;

        // Both writers should be empty
        let source_vec = source.by_ref();
        let dest_vec = dest.by_ref();

        assert_eq!(source_vec.len(), 0);
        assert_eq!(dest_vec.len(), 0);

        Ok(())
    }

    #[test]
    fn test_ingest_single_record() -> crate::Result<()> {
        // Test ingesting a single record
        let header = VBinseqHeader::new(false, false, false);

        // Create a source writer with a single record
        let mut source = VBinseqWriterBuilder::default()
            .header(header)
            .headless(true)
            .build(Vec::new())?;

        // Write a single sequence
        let seq = b"ACGTACGTACGT";
        source.write_nucleotides(1, seq)?;

        // Create a destination writer
        let mut dest = VBinseqWriterBuilder::default()
            .header(header)
            .headless(true)
            .build(Vec::new())?;

        // Ingest from source to dest
        dest.ingest(&mut source)?;

        // Source should be empty, dest should have content
        let source_vec = source.by_ref();
        assert_eq!(source_vec.len(), 0);

        // Source ubuffer should be empty as well
        let source_ubuf = &source.cblock.ubuf;
        assert!(source_ubuf.is_empty());

        // The destination vec will be empty because we haven't hit a buffer limit
        let dest_vec = dest.by_ref();
        assert!(dest_vec.is_empty());

        // The destination ubuffer should have some data however
        let dest_ubuf = &dest.cblock.ubuf;
        assert!(!dest_ubuf.is_empty());

        Ok(())
    }

    #[test]
    fn test_ingest_multi_record() -> crate::Result<()> {
        // Test ingesting a single record
        let header = VBinseqHeader::new(false, false, false);

        // Create a source writer with a single record
        let mut source = VBinseqWriterBuilder::default()
            .header(header)
            .headless(true)
            .build(Vec::new())?;

        // Write multiple sequences
        for _ in 0..30 {
            let seq = b"ACGTACGTACGT";
            source.write_nucleotides(1, seq)?;
        }

        // Create a destination writer
        let mut dest = VBinseqWriterBuilder::default()
            .header(header)
            .headless(true)
            .build(Vec::new())?;

        // Ingest from source to dest
        dest.ingest(&mut source)?;

        // Source should be empty, dest should have content
        let source_vec = source.by_ref();
        assert_eq!(source_vec.len(), 0);

        // Source ubuffer should be empty as well
        let source_ubuf = &source.cblock.ubuf;
        assert!(source_ubuf.is_empty());

        // The destination vec will be empty because we haven't hit a buffer limit
        let dest_vec = dest.by_ref();
        assert!(dest_vec.is_empty());

        // The destination ubuffer should have some data however
        let dest_ubuf = &dest.cblock.ubuf;
        assert!(!dest_ubuf.is_empty());

        Ok(())
    }

    #[test]
    fn test_ingest_block_boundary() -> crate::Result<()> {
        // Test ingesting a single record
        let header = VBinseqHeader::new(false, false, false);

        // Create a source writer with a single record
        let mut source = VBinseqWriterBuilder::default()
            .header(header)
            .headless(true)
            .build(Vec::new())?;

        // Write multiple sequences (will cross boundary)
        for _ in 0..30000 {
            let seq = b"ACGTACGTACGT";
            source.write_nucleotides(1, seq)?;
        }

        // Create a destination writer
        let mut dest = VBinseqWriterBuilder::default()
            .header(header)
            .headless(true)
            .build(Vec::new())?;

        // Ingest from source to dest
        dest.ingest(&mut source)?;

        // Source should be empty, dest should have content
        let source_vec = source.by_ref();
        assert_eq!(source_vec.len(), 0);

        // Source ubuffer should be empty as well
        let source_ubuf = &source.cblock.ubuf;
        assert!(source_ubuf.is_empty());

        // The destination vec will be empty because we haven't hit a buffer limit
        let dest_vec = dest.by_ref();
        assert!(!dest_vec.is_empty());

        // The destination ubuffer should have some data however
        let dest_ubuf = &dest.cblock.ubuf;
        assert!(!dest_ubuf.is_empty());

        Ok(())
    }
}
