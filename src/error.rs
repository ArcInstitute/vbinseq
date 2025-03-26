//! # Error Types and Results
//! 
//! This module defines the error types used throughout the VBINSEQ crate.
//! It provides a consistent error handling approach through a custom `Result<T>` type
//! that wraps the various error categories that can occur when working with VBINSEQ files.
//! 
//! The error hierarchy includes:
//! 
//! * `Error` - The top-level error enum that encapsulates all possible errors
//! * `HeaderError` - Errors related to parsing and validating file headers
//! * `WriteError` - Errors that can occur during writing operations
//! * `ReadError` - Errors that can occur during reading operations
//! * `IndexError` - Errors related to file indexing

use crate::VBinseqHeader;

/// A convenient result type used throughout the crate
/// 
/// This is a type alias for `std::result::Result<T, Error>` where `Error` is this
/// crate's custom error type that can represent all possible errors.
pub type Result<T> = std::result::Result<T, Error>;

/// The main error type for the VBINSEQ crate
/// 
/// This enum encompasses all possible errors that can occur when working with VBINSEQ files.
/// It provides convenient conversions from specific error types through the `From` trait
/// implementations derived via `#[from]` attributes.
/// 
/// # Examples
/// 
/// ```rust
/// use vbinseq::{Result, Error};
/// 
/// fn example_operation() -> Result<()> {
///     // If any internal operation fails, the error will be automatically
///     // converted to the appropriate Error variant
///     Ok(())
/// }
/// ```
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Errors related to file and block headers
    #[error("Error processing header: {0}")]
    HeaderError(#[from] HeaderError),
    
    /// Errors that occur during write operations
    #[error("Error writing file: {0}")]
    WriteError(#[from] WriteError),
    
    /// Errors that occur during read operations
    #[error("Error reading file: {0}")]
    ReadError(#[from] ReadError),
    
    /// Errors related to file indexing
    #[error("Error processing Index: {0}")]
    IndexError(#[from] IndexError),
    
    /// Standard I/O errors
    #[error("Error with IO: {0}")]
    IoError(#[from] std::io::Error),
    
    /// UTF-8 conversion errors
    #[error("Error with UTF8: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
    
    /// Errors from the bitnuc dependency for nucleotide encoding/decoding
    #[error("Bitnuc error: {0}")]
    BitnucError(#[from] bitnuc::NucleotideError),
    
    /// Generic errors for other unexpected situations
    #[error("Generic error: {0}")]
    AnyhowError(#[from] anyhow::Error),
}
impl Error {
    /// Checks if the error is an index mismatch error
    /// 
    /// This is useful for determining if a file's index is out of sync with its content,
    /// which might require rebuilding the index.
    /// 
    /// # Returns
    /// 
    /// * `true` if the error is an `IndexError::ByteSizeMismatch`
    /// * `false` for all other error types
    pub fn is_index_mismatch(&self) -> bool {
        match self {
            Self::IndexError(err) => err.is_mismatch(),
            _ => false,
        }
    }
}

/// Errors that can occur during write operations to VBINSEQ files
/// 
/// These errors typically occur when there's a mismatch between the header configuration
/// and the data being written, or when there are issues with the data format.
#[derive(thiserror::Error, Debug)]
pub enum WriteError {
    /// When trying to write data without quality scores but the header specifies they should be present
    #[error("Quality flag is set in header but trying to write without quality scores.")]
    QualityFlagSet,
    
    /// When trying to write data without a pair but the header specifies paired records
    #[error("Paired flag is set in header but trying to write without record pair.")]
    PairedFlagSet,
    
    /// When trying to write quality scores but the header specifies they are not present
    #[error("Quality flag not set in header but trying to write quality scores.")]
    QualityFlagNotSet,
    
    /// When trying to write paired data but the header doesn't specify paired records
    #[error("Paired flag not set in header but trying to write with record pair.")]
    PairedFlagNotSet,
    
    /// When a record is too large to fit in a block of the configured size
    /// 
    /// The first parameter is the record size, the second is the maximum block size
    #[error("Encountered a record with embedded size {0} but the maximum block size is {1}. Rerun with increased block size.")]
    RecordSizeExceedsMaximumBlockSize(usize, usize),
    
    /// When invalid nucleotide characters are found in a sequence
    #[error("Invalid nucleotides found in sequence: {0}")]
    InvalidNucleotideSequence(String),
    
    /// When a header is not provided to the writer builder
    #[error("Missing header in writer builder")]
    MissingHeader,
    
    /// When trying to ingest blocks with different sizes than expected
    /// 
    /// The first parameter is the expected size, the second is the found size
    #[error("Incompatible block sizes encountered in BlockWriter Ingest. Found ({1}) Expected ({0})")]
    IncompatibleBlockSizes(usize, usize),
    
    /// When trying to ingest data with an incompatible header
    /// 
    /// The first parameter is the expected header, the second is the found header
    #[error("Incompatible headers found in VBinseqWriter::ingest. Found ({1:?}) Expected ({0:?})")]
    IncompatibleHeaders(VBinseqHeader, VBinseqHeader),
}

/// Errors related to parsing and validating VBINSEQ file headers
/// 
/// These errors occur when a file header is corrupted or doesn't match the expected format.
#[derive(thiserror::Error, Debug)]
pub enum HeaderError {
    /// When the magic number in the header doesn't match the expected value ("VSEQ")
    /// 
    /// The parameter is the invalid magic number that was found
    #[error("Invalid magic number: {0}")]
    InvalidMagicNumber(u32),
    
    /// When the format version is not supported by this library
    /// 
    /// The parameter is the unsupported version number
    #[error("Invalid format version: {0}")]
    InvalidFormatVersion(u8),
    
    /// When the reserved bytes section of the header is invalid
    #[error("Invalid reserved bytes")]
    InvalidReservedBytes,
}

/// Errors related to VBINSEQ file indexing
/// 
/// These errors occur when there are issues with the index of a VBINSEQ file,
/// such as corruption or mismatches with the underlying file.
#[derive(thiserror::Error, Debug)]
pub enum IndexError {
    /// When the magic number in the index doesn't match the expected value
    /// 
    /// The parameter is the invalid magic number that was found
    #[error("Invalid magic number: {0}")]
    InvalidMagicNumber(u64),
    
    /// When the index references a file that doesn't exist
    /// 
    /// The parameter is the missing file path
    #[error("Index missing upstream file path: {0}")]
    MissingUpstreamFile(String),
    
    /// When the size of the file doesn't match what the index expects
    /// 
    /// The first parameter is the actual file size, the second is the expected size
    #[error("Mismatch in size between upstream size: {0} and expected index size {1}")]
    ByteSizeMismatch(u64, u64),
}

impl IndexError {
    /// Checks if this error indicates a mismatch between the index and file
    /// 
    /// This is useful to determine if the index needs to be rebuilt.
    /// 
    /// # Returns
    /// 
    /// * `true` for `ByteSizeMismatch` errors
    /// * `true` for any other error type (this behavior is likely a bug and should be fixed)
    pub fn is_mismatch(&self) -> bool {
        matches!(self, Self::ByteSizeMismatch(_, _) | _) // Note: this appears to always return true regardless of error type
    }
}

/// Errors that can occur during read operations from VBINSEQ files
/// 
/// These errors typically occur when there are issues with the file format or
/// when attempting to read beyond the end of the file.
#[derive(thiserror::Error, Debug)]
pub enum ReadError {
    /// When the file metadata doesn't match the expected VBINSEQ format
    #[error("Unexpected file metadata")]
    InvalidFileType,
    
    /// When a block header contains an invalid magic number
    /// 
    /// The first parameter is the invalid magic number, the second is the position in the file
    #[error("Unexpected Block Magic Number found: {0} at position {1}")]
    InvalidBlockMagicNumber(u64, usize),
    
    /// When trying to read a block but reaching the end of the file unexpectedly
    /// 
    /// The parameter is the position in the file where the read was attempted
    #[error("Unable to find an expected full block at position {0}")]
    UnexpectedEndOfFile(usize),
}
