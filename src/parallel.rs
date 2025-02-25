use crate::{error::Result, reader::RefRecord};

/// Trait for types that can process records in parallel
pub trait ParallelProcessor: Send + Clone {
    /// Process a single record
    fn process_record(&mut self, record: RefRecord) -> Result<()>;

    /// Called when a thread finishes processing its batch
    /// Default implementation does nothing
    #[allow(unused_variables)]
    fn on_batch_complete(&mut self) -> Result<()> {
        Ok(())
    }

    /// Set the thread ID for this processor
    ///
    /// Each thread should call this method with its own unique ID.
    #[allow(unused_variables)]
    fn set_tid(&mut self, _tid: usize) {
        // Default implementation does nothing
    }

    /// Get the thread ID for this processor
    fn get_tid(&self) -> Option<usize> {
        None
    }
}
