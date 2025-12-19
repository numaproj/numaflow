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
}

impl ShmMapClient {
    pub fn new(request_buffer: ShmRingBuffer, response_buffer: ShmRingBuffer) -> Self {
        Self {
            request_buffer: Arc::new(Mutex::new(request_buffer)),
            response_buffer: Arc::new(Mutex::new(response_buffer)),
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
         
         // Spawn writer task: reads from gRPC input stream (from Numaflow) -> Writes to SHM
         tokio::spawn(async move {
             while let Some(req) = input_stream.next().await {
                 let mut buf = Vec::new();
                 if req.encode(&mut buf).is_ok() {
                     let mut framed = Vec::with_capacity(4 + buf.len());
                     framed.extend_from_slice(&(buf.len() as u32).to_le_bytes());
                     framed.extend_from_slice(&buf);
                     
                     loop {
                         let mut ring = req_buf.lock().await;
                         match ring.write(&framed) {
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
                 {
                     let ring = resp_buf.lock().await;
                     if let Ok(bytes) = ring.peek_exact(4) {
                         msg_len = Some(u32::from_le_bytes(bytes.try_into().unwrap()) as usize);
                     }
                 }
                 
                 if let Some(len) = msg_len {
                     let mut payload: Option<Vec<u8>> = None;
                     {
                         let mut ring = resp_buf.lock().await;
                         if ring.bytes_available() >= 4 + len {
                             if let Ok(data) = ring.read_exact(4 + len) {
                                 payload = Some(data[4..].to_vec());
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
