//! # VBINSEQ
//! 
//! VBINSEQ is a high-performance binary file format for nucleotides.
//! 
//! It is a variant of the BINSEQ file format with support for _variable length records_ and _quality scores_.
//! 
//! ## Overview
//! 
//! VBINSEQ provides a block-based file format for efficient storage and retrieval of nucleotide sequences.
//! Key features include:
//! 
//! * **Block-based architecture** - Data is stored in fixed-size record blocks that can be processed independently
//! * **Variable-length records** - Unlike fixed-size records, variable-length records can store sequences of any size
//! * **Quality scores** - Optional quality score tracking for each nucleotide
//! * **Paired sequences** - Support for paired-end sequencing data
//! * **Parallel compression** - Support for ZSTD compression with parallel processing
//! * **Random access** - Efficient random access to record blocks
//! 
//! ## Usage
//! 
//! The two primary interfaces are:
//! 
//! * `VBinseqWriter` - For writing nucleotide sequences to a VBINSEQ file
//! * `MmapReader` - For memory-mapped reading of VBINSEQ files
//! 
//! ### Writing to a VBINSEQ file
//! 
//! ```rust
//! use std::fs::File;
//! use std::io::BufWriter;
//! use vbinseq::{VBinseqHeader, VBinseqWriterBuilder, MmapReader};
//! 
//! // Path to the output file
//! let path_name = "some_example.vbq";
//! 
//! // Create a header with quality scores and compression enabled
//! let header = VBinseqHeader::new(true, true, false);
//! 
//! // Open a file for writing
//! let handle = File::create(path_name).map(BufWriter::new).unwrap();
//! 
//! // Create a writer with the specified header
//! let mut writer = VBinseqWriterBuilder::default()
//!     .header(header)
//!     .build(handle)
//!     .unwrap();
//! 
//! // Write a nucleotide sequence with quality scores
//! let sequence = b"ACGTACGT";
//! let quality = b"!!!?!?!!";
//! writer.write_nucleotides_quality(0, sequence, quality).unwrap();
//! writer.finish().unwrap();
//! 
//! // Open a file for memory-mapped reading
//! let mut reader = MmapReader::new(path_name).unwrap();
//! let mut block = reader.new_block();
//! 
//! // Process blocks one at a time
//! let mut seq_buffer = Vec::new();
//! while reader.read_block_into(&mut block).unwrap() {
//!     for record in block.iter() {
//!         // Decode the sequence
//!         record.decode_s(&mut seq_buffer).unwrap();
//!         println!("Sequence {}: {}", record.index(), std::str::from_utf8(&seq_buffer).unwrap());
//! 
//!         // Validate the sequence and quality scores
//!         assert_eq!(seq_buffer, sequence);
//!         assert_eq!(record.squal(), quality);
//! 
//!         seq_buffer.clear(); // Clear the buffer for the next sequence
//!     }
//! }
//! 
//! // Delete the temporary file (for testing purposes)
//! std::fs::remove_file(path_name).unwrap();
//! ```
//! 
//! ## File Format Structure
//! 
//! The VBINSEQ file format consists of:
//! 
//! 1. A file header (32 bytes) containing format information
//! 2. A series of record blocks, each containing:
//!    - Block header (32 bytes)
//!    - Block data (variable size, containing records)
//!    - Block padding (to maintain fixed virtual block size)
//! 
//! Each record contains a preamble with metadata and data containing encoded sequences and quality scores.
//! 
//! See the README.md for detailed format specifications.

pub mod error;
pub mod header;
pub mod index;
pub mod parallel;
pub mod policy;
pub mod reader;
pub mod writer;

pub use error::{Error, Result};
pub use header::{BlockHeader, VBinseqHeader};
pub use index::{BlockIndex, BlockRange};
pub use parallel::ParallelProcessor;
pub use policy::Policy;
pub use reader::{MmapReader, RefRecord};
pub use writer::{VBinseqWriter, VBinseqWriterBuilder};
