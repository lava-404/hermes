// the task ends after processing just one batch 😭 ✅ done
//either you have the vector to contain each log's len and offset along with its serialized self or you seperate each serialized log ✅ done
//implement log count ✅ done
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
pub mod main2;
use std::io::{Read, SeekFrom};
#[derive(Serialize, Deserialize)]
pub struct Log {
    pub log: String,
}

pub struct AppState {
    pub offset: u64,
    pub log_file: File,
    pub index_file: File,
    pub queue: mpsc::Sender<Log>,
    pub log_file_id: u64
}

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    
    let log_file = File::create("log_file_0")
        .expect("failed to create the file");
    
    let index_file = File::create("index_file_0")
        .expect("failed to create the file");
    let (tx, mut rx) = mpsc::channel(100);
    let shared_state = Arc::new(Mutex::new(AppState {
        offset: 0,
        log_file,
        index_file,
        queue: tx.clone(),
        log_file_id : 0
    }));
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

//writing to the files
pub fn produce2(buffer: Vec<u8>, index_buffer: Vec<u8>, shared_state: Arc<Mutex<AppState>>) -> () {
    let mut state = shared_state.blocking_lock();

    //finding the disk storage taken by the index and log files
    let meta = state.log_file.metadata().expect("failed to find metadat of log file");
    let disk_bytes = meta.st_blocks() * 512; 



    //if file size is less than 1 gb then continue adding buffer
    if disk_bytes <  1_073_741_824 {
        state.log_file
        .write_all(&buffer)
        .expect("failed to write log batch");

        state.index_file
        .write_all(&index_buffer)
        .expect("failed to write index batch");
    }
    //if file size exceeds 
    else {
        state.log_file = File::create(format!("log_file_{}", state.log_file_id + 1)).expect("failed to create new log file");
        state.index_file = File::create(format!("index_file_{}", state.log_file_id + 1)).expect("failed to create new index file");

        state.log_file
        .write_all(&buffer)
        .expect("failed to write log batch");

        state.index_file
        .write_all(&index_buffer)
        .expect("failed to write index batch");

        state.log_file_id += 1;
    }

}

//TO BE WORKED UPON 
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
    let (log, _) = bincode::serde::decode_from_slice::<Log, _>(
        &log_bytes,
        bincode::config::standard()
    ).unwrap();
    Some(log.log)
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


//[offset][len][serialized_log]


/*i have one doubt, now in my file, the logs get saved as:   [offset][len][serialized_log][offset][len][serialized_log][offset][len][serialized_log][offset][len][serialized_log][offset][len][serialized_log]. so when we are trying to find a log at a given offset, cant we directly look the offset up in the index file and the based on the previous multiple of 100 (because index file stores the logs in batch of 100s with positions, then calculate how far the position of the cursor is from the streaming position then read the len and send it back? */