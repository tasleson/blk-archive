use std::io::{Read, Result as IoResult};

/// A streaming GearHash-based CDC implementation that mimics the FastCDC StreamCDC API.
/// This reads from a Reader and yields chunk lengths, similar to fastcdc::v2020::StreamCDC.
pub struct GearHashStreamCDC<R: Read> {
    reader: R,
    hasher: gearhash::Hasher<'static>,
    min_size: usize,
    avg_size: usize,
    max_size: usize,
    mask_s: u64,
    mask_l: u64,
    buffer: Vec<u8>,
    buffer_pos: usize,
    total_read: usize,
    finished: bool,
}

impl<R: Read> GearHashStreamCDC<R> {
    pub fn new(reader: R, min_size: u32, avg_size: u32, max_size: u32) -> Self {
        let shift = 36;
        let mask_s = ((avg_size as u64 * 2) - 1) << shift;
        let mask_l = ((avg_size as u64 / 2) - 1) << shift;

        Self {
            reader,
            hasher: gearhash::Hasher::default(),
            min_size: min_size as usize,
            avg_size: avg_size as usize,
            max_size: max_size as usize,
            mask_s,
            mask_l,
            buffer: vec![0u8; 64 * 1024], // 64KB read buffer
            buffer_pos: 0,
            total_read: 0,
            finished: false,
        }
    }

    fn read_more(&mut self) -> IoResult<usize> {
        if self.buffer_pos < self.buffer.len() {
            // Still have space in buffer
            let n = self.reader.read(&mut self.buffer[self.buffer_pos..])?;
            if n == 0 {
                self.finished = true;
            }
            self.buffer_pos += n;
            Ok(n)
        } else {
            // Buffer is full, need to consume some first
            Ok(0)
        }
    }

    fn find_boundary_in_buffer(&mut self, start: usize) -> Option<usize> {
        if start >= self.buffer_pos {
            return None;
        }

        let available = self.buffer_pos - start;
        let mut current = start;
        let mut consumed = 0;

        // Phase 1: Look for small chunks (from min_size to avg_size)
        if consumed < self.avg_size && available > 0 {
            let scan_start = self.min_size.saturating_sub(consumed);
            let scan_end = std::cmp::min(available, self.avg_size.saturating_sub(consumed));

            if scan_end > scan_start {
                if let Some(boundary) = self.hasher.next_match(
                    &self.buffer[current + scan_start..current + scan_end],
                    self.mask_s,
                ) {
                    return Some(start + scan_start + boundary);
                }
                consumed += scan_end;
                current += scan_end;
            }
        }

        // Phase 2: Look for large chunks (from avg_size to max_size)
        if consumed >= self.avg_size && consumed < self.max_size && available > consumed - start {
            let scan_end = std::cmp::min(available, self.max_size.saturating_sub(consumed));

            if scan_end > consumed - start {
                if let Some(boundary) = self.hasher.next_match(
                    &self.buffer[current..current + scan_end - (consumed - start)],
                    self.mask_l,
                ) {
                    return Some(current + boundary);
                }
            }
        }

        None
    }
}

pub struct Chunk {
    pub offset: u64,
    pub length: usize,
}

impl<R: Read> Iterator for GearHashStreamCDC<R> {
    type Item = IoResult<Chunk>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished && self.buffer_pos == 0 {
            return None;
        }

        let chunk_start = 0;

        loop {
            // Try to find a boundary in current buffer
            if let Some(boundary) = self.find_boundary_in_buffer(chunk_start) {
                let chunk_len = boundary - chunk_start;

                // Create the chunk
                let chunk = Chunk {
                    offset: self.total_read as u64,
                    length: chunk_len,
                };

                // Update state
                self.total_read += chunk_len;

                // Shift buffer to remove consumed data
                if boundary < self.buffer_pos {
                    self.buffer.copy_within(boundary..self.buffer_pos, 0);
                    self.buffer_pos -= boundary;
                } else {
                    self.buffer_pos = 0;
                }

                return Some(Ok(chunk));
            }

            // No boundary found in current buffer
            let accumulated = self.buffer_pos - chunk_start;

            // If we've accumulated max_size, force a chunk
            if accumulated >= self.max_size {
                let chunk = Chunk {
                    offset: self.total_read as u64,
                    length: self.max_size,
                };

                self.total_read += self.max_size;
                self.buffer.copy_within(self.max_size..self.buffer_pos, 0);
                self.buffer_pos -= self.max_size;

                return Some(Ok(chunk));
            }

            // Try to read more data
            match self.read_more() {
                Ok(0) => {
                    // No more data available
                    if self.buffer_pos > 0 {
                        // Return remaining data as final chunk
                        let chunk = Chunk {
                            offset: self.total_read as u64,
                            length: self.buffer_pos,
                        };
                        self.total_read += self.buffer_pos;
                        self.buffer_pos = 0;
                        return Some(Ok(chunk));
                    } else {
                        // No data left
                        return None;
                    }
                }
                Ok(_) => {
                    // Read more data, continue loop
                    continue;
                }
                Err(e) => {
                    return Some(Err(e));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_basic_chunking() {
        let data = vec![0u8; 16384];
        let cursor = Cursor::new(data);

        let chunker = GearHashStreamCDC::new(cursor, 2048, 8192, 32768);

        let chunks: Vec<_> = chunker.collect();
        assert!(!chunks.is_empty());

        // Total length should equal input
        let total: usize = chunks.iter().map(|c| c.as_ref().unwrap().length).sum();
        assert_eq!(total, 16384);
    }

    #[test]
    fn test_small_data() {
        let data = vec![1u8; 1024];
        let cursor = Cursor::new(data);

        let chunker = GearHashStreamCDC::new(cursor, 256, 512, 2048);

        let chunks: Vec<_> = chunker.collect();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].as_ref().unwrap().length, 1024);
    }

    #[test]
    fn test_empty_data() {
        let data = vec![];
        let cursor = Cursor::new(data);

        let chunker = GearHashStreamCDC::new(cursor, 256, 512, 2048);

        let chunks: Vec<_> = chunker.collect();
        assert!(chunks.is_empty());
    }
}
