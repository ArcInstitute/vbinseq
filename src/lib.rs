pub mod error;
pub mod header;
pub mod writer;

pub use error::{Error, Result};
pub use header::{BlockHeader, VBinseqHeader};
pub use writer::VBinseqWriter;
