use std::fs::File;
use std::io::{Seek, Write};
use std::sync::Arc;

use futures_util::StreamExt;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use std::io::{Read, SeekFrom};
#[derive(Serialize)]
pub struct Log {
    pub log: String,
}

pub struct AppState {
    pub offset: u64,
    pub log_file: File,
    pub index_file: File
}

#[tokio::main]
async fn main() {
    println!("Hello, world!");

    let log_file = File::create("log_file")
        .expect("failed to create the file");

    let index_file = File::create("index_file")
        .expect("failed to create the file");

    let shared_state = Arc::new(Mutex::new(AppState {
        offset: 0,
        log_file,
        index_file
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

pub fn produce(
    log: Log,
    shared_state: Arc<Mutex<AppState>>,
) {
    // serialize the log
    let binding = serde_json::to_string(&log)
        .expect("failed to serialize to JSON string");

    let serialized_log = binding.as_bytes();

    let mut state = shared_state.blocking_lock();

    let position = state.log_file.stream_position().expect("failed to stream position of the file");

    let len = serde_json::to_string(&log).expect("failed to serialize log to string").len().to_le_bytes();

    let offset = state.offset.to_le_bytes();

    let offset_int: u64 = u64::from_le_bytes(state.offset.to_le_bytes());   

    // write the offset
    state.log_file
        .write_all(&offset)
        .expect("failed to write offset to the file");

    // write the log len
    state.log_file
        .write_all(&len)
        .expect("failed to write offset to the file");
    
    // write the log into the file
    state.log_file
        .write_all(serialized_log)
        .expect("failed to write into file");


    //write offset into index file for every 100 logs
    if offset_int % 100 == 0 {
        state.index_file
            .write_all(&offset)
            .expect("failed to write offset to the file"); 

        //write position into index file
        state.index_file
            .write_all(&position.to_le_bytes())
            .expect("failed to write index to the index file");

    }

    // update offset
    state.offset += 1;
}



pub fn consume(offset: u64, shared_state: Arc<Mutex<AppState>>) -> Option<String>{
    let mut state = shared_state.blocking_lock();
    //need to go from 100 to 157
    let mut remainder = offset % 100;
    let position = find_position(&mut state.index_file, offset).expect("unable to find log") + 8;
    _ = state.log_file.seek(SeekFrom::Start(position));
    while remainder > 0 {
        let mut len_bytes = [0u8; 8];
        state.log_file.read_exact(&mut len_bytes).ok().unwrap();
        let len = u64::from_le_bytes(len_bytes);
        let mut log_bytes = vec![0u8; len as usize];
        state.log_file.read_exact(&mut log_bytes).ok().unwrap();
        remainder -= 1;
    }
        let mut len_bytes = [0u8; 8];
        state.log_file.read_exact(&mut len_bytes).ok().unwrap();
        let len = u64::from_le_bytes(len_bytes);
        let mut log_bytes = vec![0u8; len as usize];
        state.log_file.read_exact(&mut log_bytes).ok().unwrap();
    let log = String::from_utf8(log_bytes)
        .expect("invalid UTF-8");
    Some(log)
}

//gives position of 100
pub fn find_position(index_file: &mut File, target_offset: u64) -> Option<u64> {
    // Each index entry is:
    // [8 bytes offset][8 bytes position]
    let entry_size = 16;

    // Find how many index entries exist in the file.
    let file_size = index_file.metadata().ok()?.len();
    let entry_count = file_size / entry_size;

    let mut low = 0;
    let mut high = entry_count;

    // Binary search for the largest indexed offset <= target_offset
    while low < high {
        let mid = (low + high) / 2;

        // Jump directly to the middle index entry
        index_file
            .seek(SeekFrom::Start(mid * entry_size))
            .ok()?;

        let mut offset_bytes = [0u8; 8];
        let mut position_bytes = [0u8; 8];

        index_file.read_exact(&mut offset_bytes).ok()?;
        index_file.read_exact(&mut position_bytes).ok()?;

        let offset = u64::from_le_bytes(offset_bytes);
        let _ = u64::from_le_bytes(position_bytes);

        if offset <= target_offset {
            // This offset is valid, but there might be
            // a closer one further to the right.
            low = mid + 1;
        } else {
            // This offset is too large.
            high = mid;
        }
    }

    // No indexed offset <= target_offset
    if low == 0 {
        return None;
    }

    // Go back to the last valid indexed entry.
    let index = low - 1;

    index_file
        .seek(SeekFrom::Start(index * entry_size))
        .ok()?;

    let mut offset_bytes = [0u8; 8];
    let mut position_bytes = [0u8; 8];

    index_file.read_exact(&mut offset_bytes).ok()?;
    index_file.read_exact(&mut position_bytes).ok()?;

    Some(u64::from_le_bytes(position_bytes))
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


  //for storing index-position, store only every 100 log messages
