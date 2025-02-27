pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Error processing header: {0}")]
    HeaderError(#[from] HeaderError),

    #[error("Error writing file: {0}")]
    WriteError(#[from] WriteError),

    #[error("Error reading file: {0}")]
    ReadError(#[from] ReadError),

    #[error("Error with IO: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Bitnuc error: {0}")]
    BitnucError(#[from] bitnuc::NucleotideError),
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
pub enum ReadError {
    #[error("Unexpected file metadata")]
    InvalidFileType,

    #[error("Unexpected Block Magic Number found: {0} at position {1}")]
    InvalidBlockMagicNumber(u64, usize),

    #[error("Unable to find an expected full block at position {0}")]
    UnexpectedEndOfFile(usize),
}
