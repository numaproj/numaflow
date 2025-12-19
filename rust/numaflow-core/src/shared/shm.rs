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

/// Magic u32 to identify the start of a valid Numaflow SHM message.
/// "NUMA" in ascii = 0x4E554D41? No, let's use 0xBADFACE0 as a placeholder or proper magic.
/// Let's use "NMFL" = 0x4E4D464C
pub const SHM_MAGIC: u32 = 0x4E4D464C;
pub const SHM_VERSION: u16 = 1;

/// Fixed-size header that precedes every SHM message.
/// Wire format:
/// [Magic: 4] [Version: 2] [Length: 4] [Epoch: 8] [GenID: 8] [Partition: 2] [Reserved: 4] = 32 bytes
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShmPacketHeader {
    pub magic: u32,
    pub version: u16,
    pub length: u32,
    pub epoch_id: u64,
    pub generation_id: u64,
    pub partition_id: u16,
    pub _reserved: u32, // Padding to 32 bytes
}

impl ShmPacketHeader {
    pub const SIZE: usize = 32;

    pub fn new(length: u32, epoch_id: u64, generation_id: u64, partition_id: u16) -> Self {
        Self {
            magic: SHM_MAGIC,
            version: SHM_VERSION,
            length,
            epoch_id,
            generation_id,
            partition_id,
            _reserved: 0,
        }
    }

    pub fn to_le_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..10].copy_from_slice(&self.length.to_le_bytes());
        buf[10..18].copy_from_slice(&self.epoch_id.to_le_bytes());
        buf[18..26].copy_from_slice(&self.generation_id.to_le_bytes());
        buf[26..28].copy_from_slice(&self.partition_id.to_le_bytes());
        // reserved 28..32 is zero
        buf
    }

    pub fn from_le_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != SHM_MAGIC {
            return None;
        }
        let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        let length = u32::from_le_bytes(buf[6..10].try_into().unwrap());
        let epoch_id = u64::from_le_bytes(buf[10..18].try_into().unwrap());
        let generation_id = u64::from_le_bytes(buf[18..26].try_into().unwrap());
        let partition_id = u16::from_le_bytes(buf[26..28].try_into().unwrap());

        Some(Self {
            magic,
            version,
            length,
            epoch_id,
            generation_id,
            partition_id,
            _reserved: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shm_packet_header_roundtrip() {
        let original = ShmPacketHeader::new(1024, 123456789, 987654321, 55);
        let bytes = original.to_le_bytes();
        assert_eq!(bytes.len(), ShmPacketHeader::SIZE);
        
        let deserialized = ShmPacketHeader::from_le_bytes(&bytes).expect("Should deserialize");
        assert_eq!(original, deserialized);
        assert_eq!(deserialized.magic, SHM_MAGIC);
        assert_eq!(deserialized.version, SHM_VERSION);
        assert_eq!(deserialized.length, 1024);
        assert_eq!(deserialized.epoch_id, 123456789);
        assert_eq!(deserialized.generation_id, 987654321);
        assert_eq!(deserialized.partition_id, 55);
    }

    #[test]
    fn test_shm_packet_header_invalid_magic() {
        let mut bytes = [0u8; ShmPacketHeader::SIZE];
        // Set bad magic
        bytes[0] = 0xAA; 
        assert!(ShmPacketHeader::from_le_bytes(&bytes).is_none());
    }
}


