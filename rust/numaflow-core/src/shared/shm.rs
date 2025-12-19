#![allow(unsafe_code)]
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::slice;
use memmap2::MmapMut;
use crate::error::{Error, Result};

/// A shared memory ring buffer for zero-copy IPC.
/// 
/// Layout:
/// - Head (AtomicUsize): 8 bytes
/// - Tail (AtomicUsize): 8 bytes
/// - Capacity (usize): 8 bytes
/// - Data: [u8; capacity]
pub struct ShmRingBuffer {
    mmap: MmapMut,
    capacity: usize,
}

#[repr(C)]
struct RingBufferHeader {
    head: AtomicUsize,
    tail: AtomicUsize,
    capacity: usize,
}

impl ShmRingBuffer {
    pub fn new<P: AsRef<Path>>(path: P, size: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .map_err(|e| Error::SharedMemory(format!("Failed to open shm file: {}", e).into()))?;

        file.set_len((size + std::mem::size_of::<RingBufferHeader>()) as u64)
            .map_err(|e| Error::SharedMemory(format!("Failed to set shm file length: {}", e).into()))?;

        let mut mmap = unsafe {
             MmapMut::map_mut(&file)
                 .map_err(|e| Error::SharedMemory(format!("Failed to mmap shm file: {}", e).into()))?
        };

        // Initialize header if new (naive check, could be improved)
        let header = unsafe { &mut *(mmap.as_mut_ptr() as *mut RingBufferHeader) };
        if header.capacity == 0 {
            header.capacity = size;
            header.head = AtomicUsize::new(0);
            header.tail = AtomicUsize::new(0);
        }

        Ok(Self {
            mmap,
            capacity: size,
        })
    }

    fn header(&self) -> &RingBufferHeader {
        unsafe { &*(self.mmap.as_ptr() as *const RingBufferHeader) }
    }

    fn data_mut(&mut self) -> &mut [u8] {
        unsafe {
            let ptr = self.mmap.as_mut_ptr().add(std::mem::size_of::<RingBufferHeader>());
            slice::from_raw_parts_mut(ptr, self.capacity)
        }
    }

    pub fn write(&mut self, data: &[u8]) -> Result<usize> {
        let (head, tail) = {
            let header = self.header();
            (header.head.load(Ordering::Acquire), header.tail.load(Ordering::Acquire))
        };
        let len = data.len();

        let available = if head >= tail {
             self.capacity - (head - tail)
        } else {
             tail - head
        };

        // Always keep 1 byte empty to distinguish full vs empty
        if len >= available {
            return Ok(0);
        }

        let capacity = self.capacity;
        {
            let buffer = self.data_mut();
            
            // Circular write
            let first_chunk = std::cmp::min(len, capacity - head);
            buffer[head..head + first_chunk].copy_from_slice(&data[0..first_chunk]);
            
            if first_chunk < len {
                buffer[0..(len - first_chunk)].copy_from_slice(&data[first_chunk..len]);
            }
        }

        let header = self.header();
        header.head.store((head + len) % self.capacity, Ordering::Release);
        Ok(len)
    }

    pub fn bytes_available(&self) -> usize {
        let (head, tail) = {
            let header = self.header();
            (header.head.load(Ordering::Acquire), header.tail.load(Ordering::Acquire))
        };
        if head >= tail {
            head - tail
        } else {
            self.capacity - (tail - head)
        }
    }

    /// Reads exactly `len` bytes from the buffer. Returns Error if not enough data.
    pub fn read_exact(&mut self, len: usize) -> Result<Vec<u8>> {
        if self.bytes_available() < len {
             return Err(Error::SharedMemory("Not enough bytes available".into()));
        }

        let tail = self.header().tail.load(Ordering::Acquire);
        let capacity = self.capacity;
        let mut result = vec![0u8; len];
        
        // Circular read
        let first_chunk = std::cmp::min(len, capacity - tail);
        // data() helper needed? We have data_mut. 
        // We can just cast mmap pointer.
        // Or change data_mut to data(&self) -> &[u8]
        let buffer_ptr = self.mmap.as_ptr();
        let buffer_offset = std::mem::size_of::<RingBufferHeader>();
        
        unsafe {
             let data_ptr = buffer_ptr.add(buffer_offset);
             let chunk1_src = data_ptr.add(tail);
             std::ptr::copy_nonoverlapping(chunk1_src, result.as_mut_ptr(), first_chunk);
             
             if first_chunk < len {
                 let chunk2_src = data_ptr; // Start of buffer
                 let remaining = len - first_chunk;
                 std::ptr::copy_nonoverlapping(chunk2_src, result.as_mut_ptr().add(first_chunk), remaining);
             }
        }
        
        // Update Tail
        self.header().tail.store((tail + len) % self.capacity, Ordering::Release);
        
        Ok(result)
    }

    /// Peeks exactly `len` bytes without advancing tail.
    pub fn peek_exact(&self, len: usize) -> Result<Vec<u8>> {
         if self.bytes_available() < len {
             return Err(Error::SharedMemory("Not enough bytes to peek".into()));
        }

        let tail = self.header().tail.load(Ordering::Acquire);
        let capacity = self.capacity;
        let mut result = vec![0u8; len];
        
        let buffer_ptr = self.mmap.as_ptr();
        let buffer_offset = std::mem::size_of::<RingBufferHeader>();
        
        // Define first_chunk before unsafe block
        let first_chunk = std::cmp::min(len, capacity - tail);
        
        unsafe {
             let data_ptr = buffer_ptr.add(buffer_offset);
             let chunk1_src = data_ptr.add(tail);
             std::ptr::copy_nonoverlapping(chunk1_src, result.as_mut_ptr(), first_chunk);

             if first_chunk < len {
                 let chunk2_src = data_ptr; 
                 let remaining = len - first_chunk;
                 std::ptr::copy_nonoverlapping(chunk2_src, result.as_mut_ptr().add(first_chunk), remaining);
             }
        }
        Ok(result)
    }
}
