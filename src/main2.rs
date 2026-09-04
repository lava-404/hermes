use std::fs::File;
use std::io::{Seek, Write};
use std::mem::take;
use std::os::macos::fs::MetadataExt;
use std::sync::Arc;
use tokio::sync::mpsc;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use std::io::{Read, SeekFrom};

#[derive(Serialize, Deserialize)]
pub struct Log {
  pub log: String,
}
pub struct Segment {
  pub seg_id: u64,
  pub first_offset: u64,
  pub last_offset: u64,
  pub log_file: File,
  pub index_file: File
}

pub struct AppState {
  pub queue: mpsc::Sender<Log>
}

pub struct LogWriter {
  pub current_segment: Segment,
  pub log_batch: Vec<u8>,
  pub index_batch: Vec<u8>,
}

impl LogWriter {
   fn append_log(&self, log: Log) {
    
  }
}


#[tokio::main]
async fn main() {
    println!("Hello, world!");
    
    let log_file = File::create("log_file_0")
        .expect("failed to create the file");
    
    let index_file = File::create("index_file_0")
        .expect("failed to create the file");
    let (tx, mut rx) = mpsc::channel(100);

    // listener for TCP stream
    let listener = TcpListener::bind("127.0.0.1:8080")
        .await
        .unwrap();

    let writer_state = Arc::clone(&shared_state);


    
    let mut buffer1: Vec<u8> = Vec::new();
    let mut index_buffer: Vec<u8> = Vec::new();

    //task running in bg to recieve logs from the queue and send them to produce in batches of 100
    tokio::spawn(async move {
        loop {
            let mut batch_count = 0;
            let state_for_write = Arc::clone(&writer_state);
            while batch_count <= 100 {
                if let Some(log) = rx.recv().await {
                    let serialized_log = bincode::serde::encode_to_vec(&log, bincode::config::standard()).unwrap();

                    let mut state = writer_state.blocking_lock();

                    let position = state.log_file.stream_position().expect("failed to stream position of the file").to_le_bytes();

                    let len = (serialized_log.len() as u64).to_le_bytes();

                    state.offset = state.offset + 1;

                    let offset = state.offset.to_le_bytes();

                    // [offset][length][payload]
                    buffer1.extend_from_slice(&offset);
                    buffer1.extend_from_slice(&len);
                    buffer1.extend_from_slice(&serialized_log);

                    if state.offset % 100 == 0 {
                        index_buffer.extend_from_slice(&offset);
                        index_buffer.extend_from_slice(&position);
                    }

                    batch_count += 1;

                    
                }else{
                    break;
                }
            }
            produce2(take(&mut buffer1), take(&mut index_buffer),  state_for_write);
        }
    }).await.unwrap();  

    // accepts connection request
    while let Ok((stream, _)) = listener.accept().await {
        // upgrades TCP stream to WebSocket connection
        let ws_stream = accept_async(stream)
            .await
            .expect("handshake failed");
        let (_, mut read) = ws_stream.split();

        //reading the logs incoming from the websocket
        let read_task = async {
            while let Some(message) = read.next().await {
                let message = message.expect("failed to unwrap message");
                if let Message::Text(text) = message {
                    let log = Log {
                        log: text.to_string(),
                    };
                    //sending the logs into the queue
                    Arc::clone(&shared_state).lock().await.queue.send(log).await.expect("failed to send log to the queue");                 
                }
            }
        };
        read_task.await;
    }

    // appending_into_the_log(log, shared_state);
}


