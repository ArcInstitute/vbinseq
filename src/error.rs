use std::io;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Error processing header: {0}")]
    HeaderError(#[from] HeaderError),

    #[error("Error with IO: {0}")]
    IoError(#[from] io::Error),
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
