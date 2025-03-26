//! Writer implementation for VBINSEQ files
//!
//! This module provides functionality for writing sequence data to VBINSEQ files,
//! including support for compression, quality scores, and paired-end reads.
//!
//! The VBINSEQ writer implements a block-based approach where records are packed
//! into fixed-size blocks. Each block has a header containing metadata about the
//! records it contains. Blocks may be optionally compressed using zstd compression.
//!
//! # Example
//!
//! ```rust,no_run
//! use vbinseq::{VBinseqWriterBuilder, VBinseqHeader};
//! use std::fs::File;
//!
//! // Create a VBINSEQ file writer
//! let file = File::create("example.vbq").unwrap();
//! let header = VBinseqHeader::with_capacity(128 * 1024, true, true, true);
//! 
//! let mut writer = VBinseqWriterBuilder::default()
//!     .header(header)
//!     .build(file)
//!     .unwrap();
//!
//! // Write a nucleotide sequence
//! let sequence = b"ACGTACGTACGT";
//! writer.write_nucleotides(0, sequence).unwrap();
//!
//! // Writer will automatically flush when dropped
//! ```

use std::io::Write;

use byteorder::{LittleEndian, WriteBytesExt};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use zstd::Encoder as ZstdEncoder;

use crate::error::{Result, WriteError};
use crate::header::{BlockHeader, VBinseqHeader};
use crate::Policy;

/// Random number generator seed used for encoding
///
/// This is a fixed seed to ensure deterministic encoding across different runs.
pub const RNG_SEED: u64 = 42;

/// Calculates the storage size in bytes required for a record without quality scores
///
/// This function calculates the total size needed to store a record in the VBINSEQ format,
/// including the flag, sequence lengths, and the encoded sequence data. The formula
/// used is: `S = w(Cs + Cx + 3)` where:
///
/// - `w`: Word size (8 bytes)
/// - `Cs`: Chunk size of the primary sequence in 64-bit words
/// - `Cx`: Chunk size of the extended sequence in 64-bit words (for paired-end reads)
/// - `3`: Additional words for flag, primary length, and extended length
///
/// # Parameters
///
/// * `schunk` - Number of 64-bit words needed for the primary sequence
/// * `xchunk` - Number of 64-bit words needed for the extended sequence (0 for single-end)
///
/// # Returns
///
/// The total size in bytes needed to store the record
///
/// # Examples
///
/// ```
/// use vbinseq::writer::record_byte_size;
///
/// // Calculate storage for a single-end read with 2 sequence chunks
/// let size = record_byte_size(2, 0);
/// assert_eq!(size, 8 * (2 + 0 + 3)); // 40 bytes
/// ```
pub fn record_byte_size(schunk: usize, xchunk: usize) -> usize {
    8 * (schunk + xchunk + 3)
}

/// Calculates the storage size in bytes required for a record with quality scores
///
/// This function extends `record_byte_size` to include the additional space
/// needed for quality scores, which require 1 byte per nucleotide base.
///
/// # Parameters
///
/// * `schunk` - Number of 64-bit words needed for the primary sequence
/// * `xchunk` - Number of 64-bit words needed for the extended sequence (0 for single-end)
/// * `slen` - Length of the primary sequence in bases
/// * `xlen` - Length of the extended sequence in bases (0 for single-end)
///
/// # Returns
///
/// The total size in bytes needed to store the record with quality scores
///
/// # Examples
///
/// ```
/// use vbinseq::writer::record_byte_size_quality;
///
/// // Calculate storage for a single-end read with 2 sequence chunks
/// // and 12 bases (which will have 12 quality score bytes)
/// let size = record_byte_size_quality(2, 0, 12, 0);
/// assert_eq!(size, (8 * (2 + 0 + 3)) + 12); // 52 bytes
/// ```
pub fn record_byte_size_quality(schunk: usize, xchunk: usize, slen: usize, xlen: usize) -> usize {
    record_byte_size(schunk, xchunk) + slen + xlen
}

/// A builder for creating configured VBinseqWriter instances
///
/// This builder provides a fluent interface for configuring and creating a
/// `VBinseqWriter` with customized settings. It allows specifying the file header,
/// encoding policy, and whether to operate in headless mode.
///
/// # Examples
///
/// ```rust,no_run
/// use vbinseq::{VBinseqWriterBuilder, VBinseqHeader, Policy};
/// use std::fs::File;
///
/// // Create a writer with custom settings
/// let file = File::create("example.vbq").unwrap();
/// let mut writer = VBinseqWriterBuilder::default()
///     .header(VBinseqHeader::with_capacity(65536, true, true, true))
///     .policy(Policy::IgnoreSequence)
///     .build(file)
///     .unwrap();
///
/// // Use the writer...
/// ```
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
    /// Sets the header for the VBINSEQ file
    ///
    /// The header defines the file format parameters such as block size, whether
    /// the file contains quality scores, paired-end reads, and compression settings.
    ///
    /// # Parameters
    ///
    /// * `header` - The VBinseqHeader to use for the file
    ///
    /// # Returns
    ///
    /// The builder with the header configured
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use vbinseq::{VBinseqWriterBuilder, VBinseqHeader};
    ///
    /// // Create a header with 64KB blocks and quality scores
    /// let mut header = VBinseqHeader::with_capacity(65536, true, true, true);
    /// header.qual = true;
    ///
    /// let builder = VBinseqWriterBuilder::default().header(header);
    /// ```
    pub fn header(mut self, header: VBinseqHeader) -> Self {
        self.header = Some(header);
        self
    }

    /// Sets the encoding policy for nucleotide sequences
    ///
    /// The policy determines how sequences are encoded into the binary format.
    /// Different policies offer trade-offs between compression ratio and compatibility
    /// with different types of sequence data.
    ///
    /// # Parameters
    ///
    /// * `policy` - The encoding policy to use
    ///
    /// # Returns
    ///
    /// The builder with the encoding policy configured
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use vbinseq::{VBinseqWriterBuilder, Policy};
    ///
    /// let builder = VBinseqWriterBuilder::default().policy(Policy::IgnoreSequence);
    /// ```
    pub fn policy(mut self, policy: Policy) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Sets whether to operate in headless mode
    ///
    /// In headless mode, the writer does not write a file header. This is useful
    /// when creating part of a file that will be merged with other parts later,
    /// such as in parallel writing scenarios.
    ///
    /// # Parameters
    ///
    /// * `headless` - Whether to operate in headless mode
    ///
    /// # Returns
    ///
    /// The builder with the headless mode configured
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use vbinseq::VBinseqWriterBuilder;
    ///
    /// // Create a headless writer for parallel writing
    /// let builder = VBinseqWriterBuilder::default().headless(true);
    /// ```
    pub fn headless(mut self, headless: bool) -> Self {
        self.headless = Some(headless);
        self
    }

    /// Builds a VBinseqWriter with the configured settings
    ///
    /// This finalizes the builder and creates a new VBinseqWriter instance using
    /// the provided writer and the configured settings. If any settings were not
    /// explicitly set, default values will be used.
    ///
    /// # Parameters
    ///
    /// * `inner` - The underlying writer where data will be written
    ///
    /// # Returns
    ///
    /// * `Ok(VBinseqWriter)` - A configured VBinseqWriter ready for use
    /// * `Err(_)` - If an error occurred while initializing the writer
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use vbinseq::VBinseqWriterBuilder;
    /// use std::fs::File;
    ///
    /// let file = File::create("example.vbq").unwrap();
    /// let mut writer = VBinseqWriterBuilder::default()
    ///     .build(file)
    ///     .unwrap();
    /// ```
    pub fn build<W: Write>(self, inner: W) -> Result<VBinseqWriter<W>> {
        VBinseqWriter::new(
            inner,
            self.header.unwrap_or_default(),
            self.policy.unwrap_or_default(),
            self.headless.unwrap_or(false),
        )
    }
}

/// Writer for VBINSEQ format files
///
/// The `VBinseqWriter` handles writing nucleotide sequence data to VBINSEQ files in a
/// block-based format. It manages the file structure, compression settings, and ensures
/// data is properly encoded and organized.
///
/// ## File Structure
///
/// A VBINSEQ file consists of:
/// 1. A file header that defines parameters like block size and compression settings
/// 2. A series of blocks, each with:
///    - A block header with metadata (e.g., record count)
///    - A collection of encoded records
///
/// Each block is filled with records until either the block is full or no more complete
/// records can fit. The writer automatically handles block boundaries and creates new
/// blocks as needed.
///
/// ## Usage
///
/// The writer supports multiple formats:
/// - Single-end sequences with or without quality scores
/// - Paired-end sequences with or without quality scores
///
/// It's recommended to use the `VBinseqWriterBuilder` to create and configure a writer
/// instance with the appropriate settings.
///
/// ```rust,no_run
/// use vbinseq::{VBinseqWriterBuilder, VBinseqHeader};
/// use std::fs::File;
///
/// // Create a writer for single-end reads
/// let file = File::create("example.vbq").unwrap();
/// let mut writer = VBinseqWriterBuilder::default()
///     .header(VBinseqHeader::default())
///     .build(file)
///     .unwrap();
///
/// // Write a sequence
/// let flag = 0; // No special flags
/// let sequence = b"ACGTACGTACGT";
/// writer.write_nucleotides(flag, sequence).unwrap();
///
/// // Writer automatically flushes when dropped
/// ```
#[derive(Clone)]
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

    /// Initializes the writer by writing the file header
    ///
    /// This method is called automatically during creation unless headless mode is enabled.
    /// It writes the VBinseqHeader to the underlying writer.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If the header was successfully written
    /// * `Err(_)` - If an error occurred during writing
    fn init(&mut self) -> Result<()> {
        self.header.write_bytes(&mut self.inner)?;
        Ok(())
    }

    /// Checks if the writer is configured for paired-end reads
    ///
    /// This method returns whether the writer expects paired-end reads based on the
    /// header settings. If true, you should use `write_paired_nucleotides` instead of
    /// `write_nucleotides` to write sequences.
    ///
    /// # Returns
    ///
    /// `true` if the writer is configured for paired-end reads, `false` otherwise
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use vbinseq::{VBinseqWriterBuilder, VBinseqHeader};
    /// use std::fs::File;
    ///
    /// // Create a header for paired-end reads
    /// let mut header = VBinseqHeader::default();
    /// header.paired = true;
    ///
    /// let file = File::create("paired_reads.vbq").unwrap();
    /// let writer = VBinseqWriterBuilder::default()
    ///     .header(header)
    ///     .build(file)
    ///     .unwrap();
    ///
    /// assert!(writer.is_paired());
    /// ```
    pub fn is_paired(&self) -> bool {
        self.header.paired
    }

    /// Checks if the writer is configured for quality scores
    ///
    /// This method returns whether the writer expects quality scores based on the
    /// header settings. If true, you should use methods that include quality scores
    /// (`write_nucleotides_with_quality` or `write_paired_nucleotides_with_quality`)
    /// to write sequences.
    ///
    /// # Returns
    ///
    /// `true` if the writer is configured for quality scores, `false` otherwise
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use vbinseq::{VBinseqWriterBuilder, VBinseqHeader};
    /// use std::fs::File;
    ///
    /// // Create a header for sequences with quality scores
    /// let mut header = VBinseqHeader::default();
    /// header.qual = true;
    ///
    /// let file = File::create("reads_with_quality.vbq").unwrap();
    /// let writer = VBinseqWriterBuilder::default()
    ///     .header(header)
    ///     .build(file)
    ///     .unwrap();
    ///
    /// assert!(writer.has_quality());
    /// ```
    pub fn has_quality(&self) -> bool {
        self.header.qual
    }

    /// Writes a single nucleotide sequence to the file
    ///
    /// This method encodes and writes a single nucleotide sequence to the VBINSEQ file.
    /// It automatically handles block boundaries and will create a new block if the
    /// current one cannot fit the encoded record.
    ///
    /// # Parameters
    ///
    /// * `flag` - A 64-bit flag that can store custom metadata about the sequence
    /// * `sequence` - The nucleotide sequence to write (typically ASCII: A, C, G, T, N)
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - If the sequence was successfully encoded and written
    /// * `Ok(false)` - If the sequence could not be encoded (e.g., invalid characters)
    /// * `Err(_)` - If an error occurred during writing or if the writer is configured
    ///   for quality scores or paired-end reads
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The writer is configured for quality scores (`WriteError::QualityFlagSet`)
    /// - The writer is configured for paired-end reads (`WriteError::PairedFlagSet`)
    /// - An I/O error occurred while writing
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use vbinseq::{VBinseqWriterBuilder, VBinseqHeader};
    /// use std::fs::File;
    ///
    /// let file = File::create("example.vbq").unwrap();
    /// let mut writer = VBinseqWriterBuilder::default()
    ///     .build(file)
    ///     .unwrap();
    ///
    /// // Write a sequence with a custom flag
    /// let flag = 0x1234; // Some arbitrary metadata
    /// let sequence = b"ACGTACGTACGT";
    /// writer.write_nucleotides(flag, sequence).unwrap();
    /// ```
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

    /// Writes a paired-end nucleotide sequence to the file
    ///
    /// This method encodes and writes a paired-end nucleotide sequence (two related sequences)
    /// to the VBINSEQ file. It automatically handles block boundaries and will create a new
    /// block if the current one cannot fit the encoded record.
    ///
    /// # Parameters
    ///
    /// * `flag` - A 64-bit flag that can store custom metadata about the sequence pair
    /// * `primary` - The primary nucleotide sequence (typically the forward read)
    /// * `extended` - The extended nucleotide sequence (typically the reverse read)
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - If the sequence pair was successfully encoded and written
    /// * `Ok(false)` - If the sequence pair could not be encoded
    /// * `Err(_)` - If an error occurred during writing or if the writer is not configured
    ///   for paired-end reads
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The writer is configured for quality scores (`WriteError::QualityFlagSet`)
    /// - The writer is not configured for paired-end reads (`WriteError::PairedFlagNotSet`)
    /// - An I/O error occurred while writing
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use vbinseq::{VBinseqWriterBuilder, VBinseqHeader};
    /// use std::fs::File;
    ///
    /// // Create a header for paired-end reads
    /// let mut header = VBinseqHeader::default();
    /// header.paired = true;
    ///
    /// let file = File::create("paired_reads.vbq").unwrap();
    /// let mut writer = VBinseqWriterBuilder::default()
    ///     .header(header)
    ///     .build(file)
    ///     .unwrap();
    ///
    /// // Write a paired sequence
    /// let flag = 0;
    /// let forward_read = b"ACGTACGTACGT";
    /// let reverse_read = b"TGCATGCATGCA";
    /// writer.write_nucleotides_paired(flag, forward_read, reverse_read).unwrap();
    /// ```
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

    /// Writes a nucleotide sequence with quality scores to the file
    ///
    /// This method encodes and writes a single nucleotide sequence with corresponding
    /// quality scores to the VBINSEQ file. Quality scores are typically in the Phred scale
    /// (encoded as ASCII characters). It automatically handles block boundaries and will
    /// create a new block if the current one cannot fit the encoded record.
    ///
    /// # Parameters
    ///
    /// * `flag` - A 64-bit flag that can store custom metadata about the sequence
    /// * `sequence` - The nucleotide sequence to write (typically ASCII: A, C, G, T, N)
    /// * `quality` - The quality scores corresponding to each base in the sequence
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - If the sequence and quality scores were successfully encoded and written
    /// * `Ok(false)` - If the sequence could not be encoded
    /// * `Err(_)` - If an error occurred during writing or if the writer is not configured
    ///   for quality scores
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The writer is not configured for quality scores (`WriteError::QualityFlagNotSet`)
    /// - The writer is configured for paired-end reads (`WriteError::PairedFlagSet`)
    /// - An I/O error occurred while writing
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use vbinseq::{VBinseqWriterBuilder, VBinseqHeader};
    /// use std::fs::File;
    ///
    /// // Create a header for sequences with quality scores
    /// let mut header = VBinseqHeader::default();
    /// header.qual = true;
    ///
    /// let file = File::create("reads_with_quality.vbq").unwrap();
    /// let mut writer = VBinseqWriterBuilder::default()
    ///     .header(header)
    ///     .build(file)
    ///     .unwrap();
    ///
    /// // Write a sequence with quality scores
    /// let flag = 0;
    /// let sequence = b"ACGTACGTACGT";
    /// let quality = b"IIIIIIEEEEEE"; // Example quality scores in ASCII format
    /// writer.write_nucleotides_quality(flag, sequence, quality).unwrap();
    /// ```
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

    /// Writes paired-end nucleotide sequences with quality scores to the file
    ///
    /// This method encodes and writes paired-end nucleotide sequences with their corresponding
    /// quality scores to the VBINSEQ file. It's designed for paired-end sequencing data where
    /// each fragment is sequenced from both ends. The method automatically handles block
    /// boundaries and will create a new block if the current one cannot fit the encoded record.
    ///
    /// # Parameters
    ///
    /// * `flag` - A 64-bit flag that can store custom metadata about the sequence pair
    /// * `s_seq` - The primary nucleotide sequence (typically the forward read)
    /// * `x_seq` - The extended nucleotide sequence (typically the reverse read)
    /// * `s_qual` - The quality scores corresponding to each base in the primary sequence
    /// * `x_qual` - The quality scores corresponding to each base in the extended sequence
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - If the sequences and quality scores were successfully encoded and written
    /// * `Ok(false)` - If the sequences could not be encoded
    /// * `Err(_)` - If an error occurred during writing or if the writer is not configured
    ///   for quality scores and paired-end reads
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The writer is not configured for quality scores (`WriteError::QualityFlagNotSet`)
    /// - The writer is not configured for paired-end reads (`WriteError::PairedFlagNotSet`)
    /// - An I/O error occurred while writing
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use vbinseq::{VBinseqWriterBuilder, VBinseqHeader};
    /// use std::fs::File;
    ///
    /// // Create a header for paired-end reads with quality scores
    /// let mut header = VBinseqHeader::default();
    /// header.qual = true;
    /// header.paired = true;
    ///
    /// let file = File::create("paired_reads_with_quality.vbq").unwrap();
    /// let mut writer = VBinseqWriterBuilder::default()
    ///     .header(header)
    ///     .build(file)
    ///     .unwrap();
    ///
    /// // Write paired sequences with quality scores
    /// let flag = 0;
    /// let forward_read = b"ACGTACGTACGT";
    /// let reverse_read = b"TGCATGCATGCA";
    /// let forward_quality = b"IIIIIIEEEEEE"; // Example quality scores
    /// let reverse_quality = b"EEEEEEIIIIEE"; // Example quality scores
    /// writer.write_nucleotides_quality_paired(
    ///     flag, 
    ///     forward_read, 
    ///     reverse_read, 
    ///     forward_quality, 
    ///     reverse_quality
    /// ).unwrap();
    /// ```
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

    /// Finishes writing and flushes all data to the underlying writer
    ///
    /// This method should be called when you're done writing to ensure all data
    /// is properly flushed to the underlying writer. It's automatically called
    /// when the writer is dropped, but calling it explicitly allows you to handle
    /// any errors that might occur during flushing.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If all data was successfully flushed
    /// * `Err(_)` - If an error occurred during flushing
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use vbinseq::{VBinseqWriterBuilder, VBinseqHeader};
    /// use std::fs::File;
    ///
    /// let file = File::create("example.vbq").unwrap();
    /// let mut writer = VBinseqWriterBuilder::default()
    ///     .build(file)
    ///     .unwrap();
    ///
    /// // Write some sequences...
    /// let sequence = b"ACGTACGTACGT";
    /// writer.write_nucleotides(0, sequence).unwrap();
    ///
    /// // Manually finish and check for errors
    /// if let Err(e) = writer.finish() {
    ///     eprintln!("Error flushing data: {}", e);
    /// }
    /// ```
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

    /// Ingests data from another VBinseqWriter that uses a Vec<u8> as its inner writer
    ///
    /// This method is particularly useful for parallel processing, where multiple writers
    /// might be writing to memory buffers and need to be combined into a single file. It
    /// transfers all complete blocks and any partial blocks from the other writer into this one.
    ///
    /// The method clears the other writer's buffer after ingestion, allowing it to be reused.
    ///
    /// # Parameters
    ///
    /// * `other` - Another VBinseqWriter whose inner writer is a Vec<u8>
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If ingestion was successful
    /// * `Err(_)` - If an error occurred during ingestion or if the headers are incompatible
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The headers of the two writers are not compatible (`WriteError::IncompatibleHeaders`)
    /// - An I/O error occurred during data transfer
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use vbinseq::{VBinseqWriterBuilder, VBinseqHeader};
    /// use std::fs::File;
    ///
    /// // Create a file writer
    /// let file = File::create("combined.vbq").unwrap();
    /// let mut file_writer = VBinseqWriterBuilder::default()
    ///     .build(file)
    ///     .unwrap();
    ///
    /// // Create a memory writer
    /// let mut mem_writer = VBinseqWriterBuilder::default()
    ///     .build(Vec::new())
    ///     .unwrap();
    ///
    /// // Write some data to the memory writer
    /// mem_writer.write_nucleotides(0, b"ACGTACGT").unwrap();
    ///
    /// // Ingest data from memory writer into file writer
    /// file_writer.ingest(&mut mem_writer).unwrap();
    /// ```
    pub fn ingest(&mut self, other: &mut VBinseqWriter<Vec<u8>>) -> Result<()> {
        if self.header != other.header {
            return Err(WriteError::IncompatibleHeaders(self.header, other.header).into());
        }

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

#[derive(Clone)]
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
        let header = BlockHeader::new(self.zbuf.len() as u64, self.starts.len() as u32);

        // Write the block header and compressed block
        header.write_bytes(inner)?;
        inner.write_all(&self.zbuf)?;

        Ok(())
    }

    fn flush_uncompressed<W: Write>(&mut self, inner: &mut W) -> Result<()> {
        // Build a block header (this is static in size in the uncompressed case)
        let header = BlockHeader::new(self.block_size as u64, self.starts.len() as u32);

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
        if self.block_size != other.block_size {
            return Err(
                WriteError::IncompatibleBlockSizes(self.block_size, other.block_size).into(),
            );
        }
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
#[derive(Clone)]
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
    use crate::{header::SIZE_HEADER, *};

    #[test]
    fn test_headless_writer() -> crate::Result<()> {
        let writer = VBinseqWriterBuilder::default()
            .headless(true)
            .build(Vec::new())?;
        assert_eq!(writer.inner.len(), 0);

        let writer = VBinseqWriterBuilder::default()
            .headless(false)
            .build(Vec::new())?;
        assert_eq!(writer.inner.len(), SIZE_HEADER);

        Ok(())
    }

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

        // We have not crossed a boundary
        assert!(source.by_ref().is_empty());

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
        // We have not crossed a boundary
        assert!(source.by_ref().is_empty());

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

        // We have crossed a boundary
        assert!(!source.by_ref().is_empty());

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

        // The destination vec will not be empty because we hit a buffer limit
        let dest_vec = dest.by_ref();
        assert!(!dest_vec.is_empty());

        // The destination ubuffer should have some data however
        let dest_ubuf = &dest.cblock.ubuf;
        assert!(!dest_ubuf.is_empty());

        Ok(())
    }

    #[test]
    fn test_ingest_with_quality_scores() -> crate::Result<()> {
        // Test ingesting records with quality scores
        let source_header = VBinseqHeader::new(true, false, false); // with quality
        let dest_header = VBinseqHeader::new(true, false, false); // with quality

        // Create a source writer with quality scores
        let mut source = VBinseqWriterBuilder::default()
            .header(source_header)
            .headless(true)
            .build(Vec::new())?;

        // Write sequences with quality scores
        for i in 0..5 {
            let seq = b"ACGTACGTACGT";
            // Simple quality scores (all the same for this test)
            let qual = vec![40; seq.len()];
            source.write_nucleotides_quality(i, seq, &qual)?;
        }

        // Create a destination writer
        let mut dest = VBinseqWriterBuilder::default()
            .header(dest_header)
            .headless(true)
            .build(Vec::new())?;

        // Ingest from source to dest
        dest.ingest(&mut source)?;

        // Verify source is cleared
        let source_vec = source.by_ref();
        assert_eq!(source_vec.len(), 0);

        // Verify destination has content in ubuf
        let dest_ubuf = &dest.cblock.ubuf;
        assert!(!dest_ubuf.is_empty());

        Ok(())
    }

    #[test]
    fn test_ingest_with_compression() -> crate::Result<()> {
        // Test ingesting a single record
        let header = VBinseqHeader::new(false, true, false);

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

        // The destination vec will not be empty because we hit a buffer limit
        let dest_vec = dest.by_ref();
        assert!(!dest_vec.is_empty());

        // The destination ubuffer should have some data however
        let dest_ubuf = &dest.cblock.ubuf;
        assert!(!dest_ubuf.is_empty());

        Ok(())
    }

    #[test]
    fn test_ingest_incompatible_headers() -> crate::Result<()> {
        let source_header = VBinseqHeader::new(false, false, false);
        let dest_header = VBinseqHeader::new(true, false, false);

        // Create a source writer with quality scores
        let mut source = VBinseqWriterBuilder::default()
            .header(source_header)
            .headless(true)
            .build(Vec::new())?;

        // Create a destination writer
        let mut dest = VBinseqWriterBuilder::default()
            .header(dest_header)
            .headless(true)
            .build(Vec::new())?;

        // Ingest from source to dest (will error)
        assert!(dest.ingest(&mut source).is_err());

        Ok(())
    }
}
