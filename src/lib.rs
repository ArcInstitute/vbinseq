pub mod error;
pub mod header;
pub mod reader;
pub mod writer;

pub use error::{Error, Result};
pub use header::{BlockHeader, VBinseqHeader};
pub use reader::MmapReader;
pub use writer::VBinseqWriter;
