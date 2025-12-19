use std::sync::Arc;
use tokio::sync::Mutex;
use crate::shared::shm::ShmRingBuffer;
use crate::mapper::map::transport::{MapUdfClient, MapStream};
use numaflow_pb::clients::map::{self, MapRequest, MapResponse};
use tonic::{Request, Response, Status};
use prost::Message;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct ShmMapClient {
    request_buffer: Arc<Mutex<ShmRingBuffer>>,
    response_buffer: Arc<Mutex<ShmRingBuffer>>,
    generation_id: u64,
}

impl ShmMapClient {
    pub fn new(request_buffer: ShmRingBuffer, response_buffer: ShmRingBuffer, generation_id: u64) -> Self {
        Self {
            request_buffer: Arc::new(Mutex::new(request_buffer)),
            response_buffer: Arc::new(Mutex::new(response_buffer)),
            generation_id,
        }
    }
}

#[tonic::async_trait]
impl MapUdfClient for ShmMapClient {
    async fn map(
        &self,
        request: Request<ReceiverStream<MapRequest>>,
    ) -> Result<Response<MapStream>, Status> {
         let mut input_stream = request.into_inner();
         let req_buf = self.request_buffer.clone();
         let generation_id = self.generation_id;
         
         // Spawn writer task: reads from gRPC input stream (from Numaflow) -> Writes to SHM
         tokio::spawn(async move {
             while let Some(req) = input_stream.next().await {
                 let mut buf = Vec::new();
                 if req.encode(&mut buf).is_ok() {
                     loop {
                         let mut ring = req_buf.lock().await;
                         // Phase 1.4: Use write_message for Two-Phase Write ordering
                         match ring.write_message(&buf, 0, generation_id, 0) {
                             Ok(0) => {
                                 // Full, backoff
                                 drop(ring);
                                 tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                             }
                             Ok(_) => break, // Success
                             Err(e) => {
                                 tracing::error!(?e, "SHM Write Error");
                                 break;
                             }
                         }
                     }
                 }
             }
         });

         let resp_buf = self.response_buffer.clone();
         let (tx, rx) = mpsc::channel(128);
         
         // Reader Task
         tokio::spawn(async move {
             loop {
                 let mut msg_len: Option<usize> = None;
                 let header_size = crate::shared::shm::ShmPacketHeader::SIZE;
                 
                 {
                     let ring = resp_buf.lock().await;
                     if let Ok(bytes) = ring.peek_exact(header_size) {
                         if let Some(header) = crate::shared::shm::ShmPacketHeader::from_le_bytes(&bytes) {
                             // Phase 1.4: Validation
                             let magic_valid = header.magic == crate::shared::shm::SHM_MAGIC;
                             let version_valid = header.version == crate::shared::shm::SHM_VERSION;
                             let flags_valid = header.flags == 1; // Strict published check
                             let gen_valid = header.generation_id == generation_id;

                             if !magic_valid || !version_valid || !flags_valid {
                                  // Invalid header or torn write (flags=0)
                                  // Wait/Backoff if flags=0 (partially written)?
                                  // For now, treat as invalid or not ready.
                                  // Ideally we shouldn't see flags=0 unless we raced with writer who updated head?
                                  // But our writer updates head LAST. So we should only see flags=1.
                                  // If we see flags=0, it's a corruption or logic error.
                                  tracing::warn!("Invalid SHM Header (magic/flags bad). waiting.");
                                  // Should we break or continue?
                                  // If head moved but flags=0, writer failed?
                             } else if !gen_valid {
                                  // Phase 1.4: Reader Generation Validation -> Fence Self
                                  tracing::error!("Generation ID Mismatch! Expected {}, Got {}. Revoked?", generation_id, header.generation_id);
                                  // Fence: Stop consuming, Stop producing.
                                  // Return error to terminate task.
                                  return; 
                             } else {
                                  msg_len = Some(header.length as usize);
                             }
                         } else {
                             // Invalid Magic (from_le_bytes checking magic failure case? No it returns None on len or magic)
                             tracing::error!("Invalid SHM Header Bytes.");
                             return; // Terminal error (proto desync)
                         }
                     }
                 }
                 
                 if let Some(len) = msg_len {
                     let mut payload: Option<Vec<u8>> = None;
                     {
                         let mut ring = resp_buf.lock().await;
                         if ring.bytes_available() >= header_size + len {
                             if let Ok(data) = ring.read_exact(header_size + len) {
                                 payload = Some(data[header_size..].to_vec());
                             }
                         }
                     }
                     
                     if let Some(data) = payload {
                         match MapResponse::decode(&data[..]) {
                             Ok(resp) => {
                                 if tx.send(Ok(resp)).await.is_err() {
                                     break;
                                 }
                             }
                             Err(e) => {
                                 tracing::error!(?e, "Failed to decode MapResponse from SHM");
                                 if tx.send(Err(Status::internal("Decode Error"))).await.is_err() {
                                     break;
                                 }
                             }
                         }
                         continue; // Data was read, try reading next immediately
                     }
                 }
                 
                 // If no data or partial data, sleep
                 tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
             }
         });
         
         let output_stream = ReceiverStream::new(rx);
         Ok(Response::new(Box::pin(output_stream) as MapStream))
    }

    async fn wait_until_ready(&self, _request: Request<()>) -> Result<Response<map::ReadyResponse>, Status> {
        Ok(Response::new(map::ReadyResponse { ready: true }))
    }
}
