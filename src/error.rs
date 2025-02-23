use std::io;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Error processing header: {0}")]
    HeaderError(#[from] HeaderError),

    #[error("Error reading file: {0}")]
    ReadError(#[from] ReadError),

    #[error("Error with IO: {0}")]
    IoError(#[from] io::Error),

    #[error("Bitnuc error: {0}")]
    BitnucError(#[from] bitnuc::NucleotideError),
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
