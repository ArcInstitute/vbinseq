use std::{
    fs::File,
    io::{stdout, BufWriter, Write},
    sync::Arc,
    time::Instant,
};

use anyhow::Result;
use parking_lot::Mutex;
use vbinseq::{MmapReader, ParallelProcessor, RefRecord};

/// A struct for decoding VBINSEQ data back to FASTQ format.
#[derive(Clone)]
pub struct Decoder {
    /// Local values
    buffer: Vec<u8>,
    dbuf: Vec<u8>,
    local_records: usize,
    quality: Vec<u8>,

    /// Global values
    global_buffer: Arc<Mutex<Box<dyn Write + Send>>>,
    num_records: Arc<Mutex<usize>>,
}

impl Decoder {
    pub fn new(writer: Box<dyn Write + Send>) -> Self {
        let global_buffer = Arc::new(Mutex::new(writer));
        Decoder {
            buffer: Vec::new(),
            dbuf: Vec::new(),
            local_records: 0,
            quality: Vec::new(),
            global_buffer,
            num_records: Arc::new(Mutex::new(0)),
        }
    }

    pub fn num_records(&self) -> usize {
        *self.num_records.lock()
    }
}
impl ParallelProcessor for Decoder {
    fn process_record(&mut self, record: RefRecord) -> vbinseq::Result<()> {
        // clear decoding buffer
        self.dbuf.clear();

        // decode sequence
        record.decode_s(&mut self.dbuf)?;

        // resize internal quality if necessary
        let qual_buf = if record.squal().is_empty() {
            if self.quality.len() < record.slen() as usize {
                self.quality.resize(record.slen() as usize, b'?');
            }
            &self.quality[0..record.slen() as usize]
        } else {
            record.squal()
        };

        // write fastq to local buffer
        write_fastq(&mut self.buffer, &self.dbuf, qual_buf)?;

        self.local_records += 1;

        Ok(())
    }

    fn on_batch_complete(&mut self) -> vbinseq::Result<()> {
        // Lock the mutex to write to the global buffer
        {
            let mut lock = self.global_buffer.lock();
            lock.write_all(&self.buffer)?;
            lock.flush()?;
        }
        // Lock the mutex to update the number of records
        {
            let mut num_records = self.num_records.lock();
            *num_records += self.local_records;
        }

        // Clear the local buffer and reset the local record count
        self.buffer.clear();
        self.local_records = 0;
        Ok(())
    }
}

fn write_fastq<W: Write>(
    buffer: &mut W,
    sequence: &[u8],
    quality: &[u8],
) -> Result<(), std::io::Error> {
    buffer.write_all(b"@seq\n")?;
    buffer.write_all(sequence)?;
    buffer.write_all(b"\n+\n")?;
    buffer.write_all(quality)?;
    buffer.write_all(b"\n")?;
    Ok(())
}

fn match_output(path: Option<&str>) -> Result<Box<dyn Write + Send>> {
    match path {
        Some(path) => {
            let writer = File::create(path).map(BufWriter::new)?;
            Ok(Box::new(writer))
        }
        None => {
            let stdout = stdout();
            Ok(Box::new(BufWriter::new(stdout)))
        }
    }
}

fn main() -> Result<()> {
    // Parameters
    let test_file = std::env::args()
        .nth(1)
        .unwrap_or("./data/out.vbq".to_string());

    let n_threads = std::env::args()
        .nth(2)
        .unwrap_or("8".to_string())
        .parse::<usize>()?;

    // Output handle
    let writer = match_output(None)?;
    let start = Instant::now();
    let reader = MmapReader::new(&test_file)?;
    let processor = Decoder::new(writer);
    reader.process_parallel(processor.clone(), n_threads)?;
    let duration = start.elapsed();
    let n_records = processor.num_records();

    eprintln!("Time: {:?}", duration);
    eprintln!("Records: {}", n_records);
    eprintln!(
        "Throughput: {:.2}M records/s",
        n_records as f64 / duration.as_millis() as f64 * 1000.0 / 1_000_000.0
    );

    Ok(())
}
