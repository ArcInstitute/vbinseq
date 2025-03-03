use rand::Rng;

use crate::{error::WriteError, Result};

/// Policy for handling invalid nucleotide sequences
#[derive(Debug, Clone, Copy, Default)]
pub enum Policy {
    #[default]
    IgnoreSequence,
    BreakOnInvalid,
    RandomDraw,
    SetToA,
    SetToC,
    SetToG,
    SetToT,
}
impl Policy {
    fn fill_with_known(sequence: &[u8], val: u8, ibuf: &mut Vec<u8>) {
        for &n in sequence {
            ibuf.push(match n {
                b'A' | b'C' | b'G' | b'T' => n,
                _ => val,
            });
        }
    }

    fn fill_with_random<R: Rng>(sequence: &[u8], rng: &mut R, ibuf: &mut Vec<u8>) {
        for &n in sequence {
            ibuf.push(match n {
                b'A' | b'C' | b'G' | b'T' => n,
                _ => match rng.gen_range(0..4) {
                    0 => b'A',
                    1 => b'C',
                    2 => b'G',
                    3 => b'T',
                    _ => unreachable!(),
                },
            });
        }
    }

    /// Convert the sequence according to the N-policy
    ///
    /// First clears the input buffer to ensure that it is empty.
    ///
    /// Returns a boolean indicating whether the sequence should be processed further.
    /// Returns an error if the sequence should be broken on invalid nucleotides.
    ///
    /// # Arguments
    /// * `sequence` - The sequence to be converted
    /// * `ibuf` - The buffer to store the converted sequence
    /// * `rng` - The random number generator
    pub fn handle<R: Rng>(&self, sequence: &[u8], ibuf: &mut Vec<u8>, rng: &mut R) -> Result<bool> {
        // First clears the input buffer to ensure that it is empty.
        ibuf.clear();

        // Returns a boolean indicating whether the sequence should be processed further.
        match self {
            Self::IgnoreSequence => Ok(false),
            Self::BreakOnInvalid => {
                let seq_str = std::str::from_utf8(sequence)?.to_string();
                Err(WriteError::InvalidNucleotideSequence(seq_str).into())
            }
            Self::RandomDraw => {
                Self::fill_with_random(sequence, rng, ibuf);
                Ok(true)
            }
            Self::SetToA => {
                Self::fill_with_known(sequence, b'A', ibuf);
                Ok(true)
            }
            Self::SetToC => {
                Self::fill_with_known(sequence, b'C', ibuf);
                Ok(true)
            }
            Self::SetToG => {
                Self::fill_with_known(sequence, b'G', ibuf);
                Ok(true)
            }
            Self::SetToT => {
                Self::fill_with_known(sequence, b'T', ibuf);
                Ok(true)
            }
        }
    }
}
