use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use futures_util::StreamExt;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Serialize)]
pub struct Log {
    pub log: String,
}

pub struct AppState {
    pub offset: u64,
    pub log_file: File,
}

#[tokio::main]
async fn main() {
    println!("Hello, world!");

    let log_file = File::create("log_file")
        .expect("failed to create the file");

    let shared_state = Arc::new(Mutex::new(AppState {
        offset: 0,
        log_file,
    }));

    // listener for TCP stream
    let listener = TcpListener::bind("127.0.0.1:8080")
        .await
        .unwrap();

    // accepts connection request
    while let Ok((stream, _)) = listener.accept().await {
        // upgrades TCP stream to WebSocket connection
        let ws_stream = accept_async(stream)
            .await
            .expect("handshake failed");

        let (_, mut read) = ws_stream.split();

        let read_task = async {
            while let Some(message) = read.next().await {
                let message = message.expect("failed to unwrap message");
            
                if let Message::Text(text) = message {
                    let log = Log {
                        log: text.to_string(),
                    };
            
                    produce(log, Arc::clone(&shared_state));
                }
            }
        };

        read_task.await;
    }

    // appending_into_the_log(log, shared_state);
}

fn produce(
    log: Log,
    shared_state: Arc<Mutex<AppState>>,
) {
    // serialize the log
    let binding = serde_json::to_string(&log)
        .expect("failed to serialize to JSON string");

    let serialized_log = binding.as_bytes();

    let mut state = shared_state.blocking_lock();

    // write the log into the file
    state.log_file
        .write_all(serialized_log)
        .expect("failed to write into file");

    let offset = state.offset.to_le_bytes();

    // write the offset
    state.log_file
        .write_all(&offset)
        .expect("failed to write offset to the file");

    // update offset
    state.offset += 1;
}

//set up producer-consumers up
//focus on making it look like this: 
/*
WebSocket
    ↓
produce(message)
    ↓
Log Engine
    ↓
Segment
    ↓
File
 */

 //make it fast by using binary encoding instead of json serialization

 //using mutex can cause bottlenecks, eventually go ahead with 

 /*

Producers
   ↓
queue
   ↓
single log writer
   ↓
batched sequential writes
   ↓
disk

  */

  //work on segments

  //indexing the offset 