pub mod error;
pub mod header;
pub mod index;
pub mod parallel;
pub mod reader;
pub mod writer;

pub use error::{Error, Result};
pub use header::{BlockHeader, VBinseqHeader};
pub use index::{BlockIndex, BlockRange};
pub use parallel::ParallelProcessor;
pub use reader::{MmapReader, RefRecord};
pub use writer::VBinseqWriter;
