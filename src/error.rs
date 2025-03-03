pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Error processing header: {0}")]
    HeaderError(#[from] HeaderError),

    #[error("Error writing file: {0}")]
    WriteError(#[from] WriteError),

    #[error("Error reading file: {0}")]
    ReadError(#[from] ReadError),

    #[error("Error processing Index: {0}")]
    IndexError(#[from] IndexError),

    #[error("Error with IO: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Error with UTF8: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),

    #[error("Bitnuc error: {0}")]
    BitnucError(#[from] bitnuc::NucleotideError),
}
impl Error {
    pub fn is_index_mismatch(&self) -> bool {
        match self {
            Self::IndexError(err) => err.is_mismatch(),
            _ => false,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum WriteError {
    #[error("Quality flag is set in header but trying to write without quality scores.")]
    QualityFlagSet,
    #[error("Paired flag is set in header but trying to write without record pair.")]
    PairedFlagSet,
    #[error("Quality flag not set in header but trying to write quality scores.")]
    QualityFlagNotSet,
    #[error("Paired flag not set in header but trying to write with record pair.")]
    PairedFlagNotSet,
    #[error("Encountered a record with embedded size {0} but the maximum block size is {1}. Rerun with increased block size.")]
    RecordSizeExceedsMaximumBlockSize(usize, usize),
    #[error("Invalid nucleotides found in sequence: {0}")]
    InvalidNucleotideSequence(String),
}

#[derive(thiserror::Error, Debug)]
pub enum HeaderError {
    #[error("Invalid magic number: {0}")]
    InvalidMagicNumber(u32),

    #[error("Invalid format version: {0}")]
    InvalidFormatVersion(u8),

    #[error("Invalid reserved bytes")]
    InvalidReservedBytes,
}

#[derive(thiserror::Error, Debug)]
pub enum IndexError {
    #[error("Invalid magic number: {0}")]
    InvalidMagicNumber(u64),
    #[error("Index missing upstream file path: {0}")]
    MissingUpstreamFile(String),
    #[error("Mismatch in size between upstream size: {0} and expected index size {1}")]
    ByteSizeMismatch(u64, u64),
}
impl IndexError {
    pub fn is_mismatch(&self) -> bool {
        matches!(self, Self::ByteSizeMismatch(_, _) | _)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ReadError {
    #[error("Unexpected file metadata")]
    InvalidFileType,

    #[error("Unexpected Block Magic Number found: {0} at position {1}")]
    InvalidBlockMagicNumber(u64, usize),

    #[error("Unable to find an expected full block at position {0}")]
    UnexpectedEndOfFile(usize),
}
