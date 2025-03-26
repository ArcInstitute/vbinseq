use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs::File, io::Read};

use byteorder::{ByteOrder, LittleEndian};
use memmap2::Mmap;
use zstd::Decoder;

use crate::{
    error::ReadError,
    header::{SIZE_BLOCK_HEADER, SIZE_HEADER},
    BlockHeader, BlockIndex, BlockRange, ParallelProcessor, Result, VBinseqHeader,
};

/// Calculates the number of 64-bit words needed to store a nucleotide sequence of the given length
/// 
/// Nucleotides are packed into 64-bit words with 2 bits per nucleotide (32 nucleotides per word).
/// This function calculates how many 64-bit words are needed to encode a sequence of a given length.
/// 
/// # Parameters
/// 
/// * `len` - Length of the nucleotide sequence in basepairs
/// 
/// # Returns
/// 
/// The number of 64-bit words required to encode the sequence
fn encoded_sequence_len(len: u64) -> usize {
    len.div_ceil(32) as usize
}

/// A container for a block of VBINSEQ records
/// 
/// The `RecordBlock` struct represents a single block of records read from a VBINSEQ file.
/// It stores the raw data for multiple records in vectors, allowing efficient iteration
/// over the records without copying memory for each record.
/// 
/// The `RecordBlock` is reused when reading blocks sequentially from a file, with its
/// contents being cleared and replaced with each new block that is read.
/// 
/// # Examples
/// 
/// ```rust,no_run
/// use vbinseq::MmapReader;
/// 
/// let reader = MmapReader::new("example.vbq").unwrap();
/// let mut block = reader.new_block(); // Create a block with appropriate size
/// ```
pub struct RecordBlock {
    /// Index of the first record in the block
    /// This allows records to maintain their global position in the file
    index: usize,

    /// Buffer containing all record flags in the block
    /// Each record has one flag value stored at the corresponding position
    flags: Vec<u64>,

    /// Buffer containing all sequence lengths in the block
    /// For each record, two consecutive entries are stored: primary sequence length and extended sequence length
    lens: Vec<u64>,

    /// Buffer containing all packed nucleotide sequences in the block
    /// Nucleotides are encoded as 2-bit values (4 nucleotides per byte)
    sequences: Vec<u64>,

    /// Buffer containing all quality scores in the block
    /// Quality scores are stored as raw bytes, one byte per nucleotide
    qualities: Vec<u8>,

    /// Maximum size of the block in bytes
    /// This is derived from the file header's block size field
    block_size: usize,

    /// Reusable buffer for temporary storage during decompression
    /// Using a reusable buffer reduces memory allocations
    rbuf: Vec<u8>,
}
impl RecordBlock {
    /// Creates a new empty `RecordBlock` with the specified block size
    /// 
    /// The block size should match the one specified in the VBINSEQ file header
    /// for proper operation. This is typically handled automatically when using
    /// `MmapReader::new_block()`.
    /// 
    /// # Parameters
    /// 
    /// * `block_size` - Maximum size of the block in bytes
    /// 
    /// # Returns
    /// 
    /// A new empty `RecordBlock` instance
    pub fn new(block_size: usize) -> Self {
        Self {
            index: 0,
            flags: Vec::new(),
            lens: Vec::new(),
            sequences: Vec::new(),
            qualities: Vec::new(),
            block_size,
            rbuf: Vec::new(),
        }
    }

    /// Returns the number of records in this block
    /// 
    /// # Returns
    /// 
    /// The number of records currently stored in this block
    pub fn n_records(&self) -> usize {
        self.flags.len()
    }

    /// Returns an iterator over the records in this block
    /// 
    /// The iterator yields `RefRecord` instances that provide access to the record data
    /// without copying the underlying data.
    /// 
    /// # Returns
    /// 
    /// An iterator over the records in this block
    /// 
    /// # Examples
    /// 
    /// ```rust,no_run
    /// use vbinseq::MmapReader;
    /// 
    /// let mut reader = MmapReader::new("example.vbq").unwrap();
    /// let mut block = reader.new_block();
    /// reader.read_block_into(&mut block).unwrap();
    /// 
    /// // Iterate over records in the block
    /// for record in block.iter() {
    ///     println!("Record {}", record.index());
    /// }
    /// ```
    pub fn iter(&self) -> RecordBlockIter {
        RecordBlockIter::new(self)
    }

    /// Updates the starting index of the block
    /// 
    /// This is used internally to keep track of the global position of records
    /// within the file, allowing each record to maintain its original index.
    /// 
    /// # Parameters
    /// 
    /// * `index` - The index of the first record in the block
    fn update_index(&mut self, index: usize) {
        self.index = index;
    }

    /// Clears all data from the block
    /// 
    /// This method resets the block to an empty state, clearing all vectors and resetting
    /// the index to 0. This is typically used when reusing a block for reading a new block
    /// from a file.
    pub fn clear(&mut self) {
        self.index = 0;
        self.flags.clear();
        self.lens.clear();
        self.sequences.clear();
        self.qualities.clear();
    }

    /// Ingest the bytes from a block into the record block
    /// 
    /// This method takes a slice of bytes and processes it to extract
    /// the records from the block. It is used when reading a block from
    /// a file into a record block.
    /// 
    /// This is a private method used primarily for parallel processing.
    /// 
    /// # Parameters
    /// 
    /// * `bytes` - A slice of bytes containing the block data
    /// * `has_quality` - A boolean indicating whether the block contains quality scores
    /// 
    /// # Returns
    /// 
    /// A `Result` indicating success or an error
    fn ingest_bytes(&mut self, bytes: &[u8], has_quality: bool) -> Result<()> {
        let mut pos = 0;
        loop {
            // Check that we have enough bytes to at least read the flag
            // and lengths. If not, break out of the loop.
            if pos + 24 > bytes.len() {
                break;
            }

            // Read the flag and advance the position
            let flag = LittleEndian::read_u64(&bytes[pos..pos + 8]);
            pos += 8;

            // Read the primary length and advance the position
            let slen = LittleEndian::read_u64(&bytes[pos..pos + 8]);
            pos += 8;

            // Read the extended length and advance the position
            let xlen = LittleEndian::read_u64(&bytes[pos..pos + 8]);
            pos += 8;

            // No more records in the block
            if slen == 0 {
                // It is possible to end up here if the block is not full
                // In this case the flag and the length are both zero
                // and effectively blank but initialized memory.
                break;
            }

            // Add the record to the block
            self.flags.push(flag);
            self.lens.push(slen);
            self.lens.push(xlen);

            // Add the primary sequence to the block
            let mut seq = [0u8; 8];
            for _ in 0..encoded_sequence_len(slen) {
                seq.copy_from_slice(&bytes[pos..pos + 8]);
                self.sequences.push(LittleEndian::read_u64(&seq));
                pos += 8;
            }

            // Add the primary quality score to the block
            if has_quality {
                let qual_buffer = &bytes[pos..pos + slen as usize];
                self.qualities.extend_from_slice(qual_buffer);
                pos += slen as usize;
            }

            // Add the extended sequence to the block
            for _ in 0..encoded_sequence_len(xlen) {
                seq.copy_from_slice(&bytes[pos..pos + 8]);
                self.sequences.push(LittleEndian::read_u64(&seq));
                pos += 8;
            }

            // Add the extended quality score to the block
            if has_quality {
                let qual_buffer = &bytes[pos..pos + xlen as usize];
                self.qualities.extend_from_slice(qual_buffer);
                pos += xlen as usize;
            }
        }
        Ok(())
    }

    fn ingest_compressed_bytes(&mut self, bytes: &[u8], has_quality: bool) -> Result<()> {
        let mut decoder = Decoder::with_buffer(bytes)?;

        let mut pos = 0;
        loop {
            // Check that we have enough bytes to at least read the flag
            // and lengths. If not, break out of the loop.
            if pos + 24 > self.block_size {
                break;
            }

            // Pull the preambles out of the compressed block and advance the position
            let mut preamble = [0u8; 24];
            decoder.read_exact(&mut preamble)?;
            pos += 24;

            // Read the flag + lengths
            let flag = LittleEndian::read_u64(&preamble[0..8]);
            let slen = LittleEndian::read_u64(&preamble[8..16]);
            let xlen = LittleEndian::read_u64(&preamble[16..24]);

            // No more records in the block
            if slen == 0 {
                // It is possible to end up here if the block is not full
                // In this case the flag and the length are both zero
                // and effectively blank but initialized memory.
                break;
            }

            // Add the record to the block
            self.flags.push(flag);
            self.lens.push(slen);
            self.lens.push(xlen);

            // Read the sequence and advance the position
            let schunk = encoded_sequence_len(slen);
            let schunk_bytes = schunk * 8;
            self.rbuf.resize(schunk_bytes, 0);
            decoder.read_exact(&mut self.rbuf[0..schunk_bytes])?;
            for chunk in self.rbuf.chunks_exact(8) {
                let seq_part = LittleEndian::read_u64(chunk);
                self.sequences.push(seq_part);
            }
            self.rbuf.clear();
            pos += schunk_bytes;

            // Add the quality score to the block
            if has_quality {
                self.rbuf.resize(slen as usize, 0);
                decoder.read_exact(&mut self.rbuf[0..slen as usize])?;
                self.qualities.extend_from_slice(&self.rbuf);
                self.rbuf.clear();
                pos += slen as usize;
            }

            // Read the sequence and advance the position
            let xchunk = encoded_sequence_len(xlen);
            let xchunk_bytes = xchunk * 8;
            self.rbuf.resize(xchunk_bytes, 0);
            decoder.read_exact(&mut self.rbuf[0..xchunk_bytes])?;
            for chunk in self.rbuf.chunks_exact(8) {
                let seq_part = LittleEndian::read_u64(chunk);
                self.sequences.push(seq_part);
            }
            self.rbuf.clear();
            pos += xchunk_bytes;

            // Add the quality score to the block
            if has_quality {
                self.rbuf.resize(xlen as usize, 0);
                decoder.read_exact(&mut self.rbuf[0..xlen as usize])?;
                self.qualities.extend_from_slice(&self.rbuf);
                self.rbuf.clear();
                pos += xlen as usize;
            }
        }
        Ok(())
    }
}

pub struct RecordBlockIter<'a> {
    block: &'a RecordBlock,
    /// Record position in the block
    rpos: usize,
    /// Encoded sequence position in the block
    epos: usize,
}
impl<'a> RecordBlockIter<'a> {
    pub fn new(block: &'a RecordBlock) -> Self {
        Self {
            block,
            rpos: 0,
            epos: 0,
        }
    }
}
impl<'a> Iterator for RecordBlockIter<'a> {
    type Item = RefRecord<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rpos == self.block.n_records() {
            return None;
        }
        let index = (self.block.index + self.rpos) as u64;
        let flag = self.block.flags[self.rpos];
        let slen = self.block.lens[2 * self.rpos];
        let xlen = self.block.lens[(2 * self.rpos) + 1];
        let schunk = encoded_sequence_len(slen);
        let xchunk = encoded_sequence_len(xlen);

        let s_seq = &self.block.sequences[self.epos..self.epos + schunk];
        let s_qual = if self.block.qualities.is_empty() {
            &[]
        } else {
            &self.block.qualities[self.epos..self.epos + slen as usize]
        };
        self.epos += schunk;

        let x_seq = &self.block.sequences[self.epos..self.epos + xchunk];
        let x_qual = if self.block.qualities.is_empty() {
            &[]
        } else {
            &self.block.qualities[self.epos..self.epos + xlen as usize]
        };
        self.epos += xchunk;

        // update record position
        self.rpos += 1;

        Some(RefRecord::new(
            index, flag, slen, xlen, s_seq, x_seq, s_qual, x_qual,
        ))
    }
}

pub struct RefRecord<'a> {
    index: u64,
    flag: u64,
    slen: u64,
    xlen: u64,
    sbuf: &'a [u64],
    xbuf: &'a [u64],
    squal: &'a [u8],
    xqual: &'a [u8],
}
impl<'a> RefRecord<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        index: u64,
        flag: u64,
        slen: u64,
        xlen: u64,
        sbuf: &'a [u64],
        xbuf: &'a [u64],
        squal: &'a [u8],
        xqual: &'a [u8],
    ) -> Self {
        Self {
            index,
            flag,
            slen,
            xlen,
            sbuf,
            xbuf,
            squal,
            xqual,
        }
    }
    pub fn index(&self) -> u64 {
        self.index
    }
    pub fn flag(&self) -> u64 {
        self.flag
    }
    pub fn slen(&self) -> u64 {
        self.slen
    }
    pub fn xlen(&self) -> u64 {
        self.xlen
    }
    pub fn sbuf(&self) -> &[u64] {
        self.sbuf
    }
    pub fn xbuf(&self) -> &[u64] {
        self.xbuf
    }
    pub fn squal(&self) -> &[u8] {
        self.squal
    }
    pub fn xqual(&self) -> &[u8] {
        self.xqual
    }
    pub fn decode_s(&self, dbuf: &mut Vec<u8>) -> Result<()> {
        bitnuc::decode(self.sbuf, self.slen as usize, dbuf)?;
        Ok(())
    }
    pub fn decode_x(&self, dbuf: &mut Vec<u8>) -> Result<()> {
        bitnuc::decode(self.xbuf, self.xlen as usize, dbuf)?;
        Ok(())
    }
    pub fn is_paired(&self) -> bool {
        self.xlen > 0
    }
    pub fn has_quality(&self) -> bool {
        !self.squal.is_empty()
    }
}

/// Memory-mapped reader for VBINSEQ files
/// 
/// `MmapReader` provides efficient, memory-mapped access to VBINSEQ files. It allows
/// sequential reading of record blocks and supports parallel processing of records.
/// 
/// Memory mapping allows the operating system to lazily load file contents as needed,
/// which can be more efficient than standard file I/O, especially for large files.
/// 
/// # Examples
/// 
/// ```rust,no_run
/// use vbinseq::MmapReader;
/// 
/// // Open a VBINSEQ file
/// let mut reader = MmapReader::new("example.vbq").unwrap();
/// 
/// // Create a block to hold records
/// let mut block = reader.new_block();
/// 
/// // Read blocks sequentially
/// while reader.read_block_into(&mut block).unwrap() {
///     println!("Read a block with {} records", block.n_records());
///     // Process records...
/// }
/// ```
pub struct MmapReader {
    /// Path to the VBINSEQ file
    path: PathBuf,

    /// Memory-mapped file contents for efficient access
    mmap: Arc<Mmap>,

    /// Parsed header information from the file
    header: VBinseqHeader,

    /// Current cursor position in the file (in bytes)
    pos: usize,

    /// Total number of records read from the file so far
    total: usize,
}
impl MmapReader {
    /// Creates a new `MmapReader` for a VBINSEQ file
    /// 
    /// This method opens the specified file, memory-maps its contents, and reads the
    /// VBINSEQ header information. The reader is positioned at the beginning of the first
    /// record block after the header.
    /// 
    /// # Parameters
    /// 
    /// * `path` - Path to the VBINSEQ file to open
    /// 
    /// # Returns
    /// 
    /// A new `MmapReader` instance if successful
    /// 
    /// # Errors
    /// 
    /// * `ReadError::InvalidFileType` if the path doesn't point to a regular file
    /// * I/O errors if the file can't be opened or memory-mapped
    /// * Header validation errors if the file doesn't contain a valid VBINSEQ header
    /// 
    /// # Examples
    /// 
    /// ```rust,no_run
    /// use vbinseq::MmapReader;
    /// 
    /// let reader = MmapReader::new("path/to/file.vbq").unwrap();
    /// ```
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        // Verify it's a regular file before attempting to map
        let file = File::open(&path)?;
        if !file.metadata()?.is_file() {
            return Err(ReadError::InvalidFileType.into());
        }

        // Safety: The file is open and won't be modified while mapped
        let mmap = unsafe { Mmap::map(&file)? };

        // Read header from mapped memory
        let header = {
            let mut header_bytes = [0u8; SIZE_HEADER];
            header_bytes.copy_from_slice(&mmap[..SIZE_HEADER]);
            VBinseqHeader::from_bytes(&header_bytes)?
        };

        Ok(Self {
            path: PathBuf::from(path.as_ref()),
            mmap: Arc::new(mmap),
            header,
            pos: SIZE_HEADER,
            total: 0,
        })
    }

    /// Creates a new empty record block with the appropriate size for this file
    /// 
    /// This creates a `RecordBlock` with a block size matching the one specified in the 
    /// file's header, ensuring it will be able to hold a full block of records.
    /// 
    /// # Returns
    /// 
    /// A new empty `RecordBlock` instance sized appropriately for this file
    /// 
    /// # Examples
    /// 
    /// ```rust,no_run
    /// use vbinseq::MmapReader;
    /// 
    /// let reader = MmapReader::new("example.vbq").unwrap();
    /// let mut block = reader.new_block();
    /// ```
    pub fn new_block(&self) -> RecordBlock {
        RecordBlock::new(self.header.block as usize)
    }

    /// Returns the path where the index file would be located
    /// 
    /// The index file is used for random access to blocks and has the same path as
    /// the VBINSEQ file with the ".vqi" extension appended.
    /// 
    /// # Returns
    /// 
    /// The path where the index file would be located
    pub fn index_path(&self) -> PathBuf {
        let mut p = self.path.as_os_str().to_owned();
        p.push(".vqi");
        p.into()
    }

    /// Returns a copy of the file's header information
    /// 
    /// The header contains information about the file format, including whether
    /// quality scores are included, whether blocks are compressed, and whether
    /// records are paired.
    /// 
    /// # Returns
    /// 
    /// A copy of the file's `VBinseqHeader`
    pub fn header(&self) -> VBinseqHeader {
        self.header
    }

    /// Fill an existing RecordBlock with the next block of records
    ///
    /// Returns false if EOF was reached, true if the block was filled
    pub fn read_block_into(&mut self, block: &mut RecordBlock) -> Result<bool> {
        // Clear the block
        block.clear();

        // Validate the next block header is within bounds and present
        if self.pos + SIZE_BLOCK_HEADER > self.mmap.len() {
            return Ok(false);
        }
        let mut header_bytes = [0u8; SIZE_BLOCK_HEADER];
        header_bytes.copy_from_slice(&self.mmap[self.pos..self.pos + SIZE_BLOCK_HEADER]);
        let header = BlockHeader::from_bytes(&header_bytes)?;
        self.pos += SIZE_BLOCK_HEADER; // advance past the block header

        // Read the block contents
        let rbound = if self.header.compressed {
            header.size as usize
        } else {
            self.header.block as usize
        };
        if self.pos + rbound > self.mmap.len() {
            return Err(ReadError::UnexpectedEndOfFile(self.pos).into());
        }
        let block_buffer = &self.mmap[self.pos..self.pos + rbound];
        if self.header.compressed {
            block.ingest_compressed_bytes(block_buffer, self.header.qual)?;
        } else {
            block.ingest_bytes(block_buffer, self.header.qual)?;
        }

        // Update the block index
        block.update_index(self.total);

        self.pos += rbound;
        self.total += header.records as usize;

        Ok(true)
    }

    pub fn load_index(&self) -> Result<BlockIndex> {
        if self.index_path().exists() {
            match BlockIndex::from_path(self.index_path()) {
                Ok(index) => Ok(index),
                Err(e) => {
                    if e.is_index_mismatch() {
                        let index = BlockIndex::from_vbq(&self.path)?;
                        index.save_to_path(self.index_path())?;
                        Ok(index)
                    } else {
                        Err(e)
                    }
                }
            }
        } else {
            let index = BlockIndex::from_vbq(&self.path)?;
            index.save_to_path(self.index_path())?;
            Ok(index)
        }
    }
}

impl MmapReader {
    /// Process records in parallel using the provided processor
    ///
    /// This method uses the block structure to divide work among threads,
    /// with each thread processing a subset of blocks independently.
    pub fn process_parallel<P: ParallelProcessor + Clone + 'static>(
        self,
        processor: P,
        num_threads: usize,
    ) -> Result<()> {
        // Generate or load the index first
        let index = self.load_index()?;

        // Get the number of blocks
        let n_blocks = index.n_blocks();
        if n_blocks == 0 {
            return Ok(()); // Nothing to process
        }

        // Calculate block assignments
        let blocks_per_thread = n_blocks.div_ceil(num_threads);

        // Create shared resources
        let mmap = Arc::clone(&self.mmap);
        let header = self.header;

        // Spawn worker threads
        let mut handles = Vec::new();

        for thread_id in 0..num_threads {
            // Calculate this thread's block range
            let start_block = thread_id * blocks_per_thread;
            let end_block = std::cmp::min((thread_id + 1) * blocks_per_thread, n_blocks);
            if start_block > n_blocks {
                continue;
            }

            let mmap = Arc::clone(&mmap);
            let mut proc = processor.clone();
            proc.set_tid(thread_id);

            // Get block ranges for this thread
            let blocks: Vec<BlockRange> = index.ranges()[start_block..end_block].to_vec();

            let handle = std::thread::spawn(move || -> Result<()> {
                // Create block to reuse for processing (within thread)
                let mut record_block = RecordBlock::new(header.block as usize);

                // Process each assigned block
                for block_range in blocks {
                    // Clear the block for reuse
                    record_block.clear();

                    // Skip the block header to get to data
                    let block_start = block_range.start_offset as usize + SIZE_BLOCK_HEADER;
                    let block_data = &mmap[block_start..block_start + block_range.len as usize];

                    // Ingest data according to the compression setting
                    if header.compressed {
                        record_block.ingest_compressed_bytes(block_data, header.qual)?;
                    } else {
                        record_block.ingest_bytes(block_data, header.qual)?;
                    }

                    // Update the record block index
                    record_block.update_index(block_range.cumulative_records as usize);

                    // Process each record in the block
                    for record in record_block.iter() {
                        proc.process_record(record)?;
                    }

                    // Signal batch completion
                    proc.on_batch_complete()?;
                }

                Ok(())
            });

            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap()?;
        }

        Ok(())
    }
}
