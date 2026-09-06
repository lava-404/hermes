use std::fs::File;
use std::io::{Seek, Write};
use std::os::macos::fs::MetadataExt;
use std::vec;
use tokio::sync::mpsc;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
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
  pub segments: Vec<Segment>,  
  pub current_segment: usize,
  pub log_batch: Vec<u8>,
  pub index_batch: Vec<u8>,
}

impl LogWriter {

   fn append_batch(&mut self) {

    //finding the disk storage taken by the index and log files
    let meta = self.segments[self.current_segment].log_file.metadata().expect("failed to find metadata");
    let disk_bytes = meta.st_blocks() * 512;

    //if file size is less than 1 gb then continue adding buffer
    if disk_bytes + self.log_batch.len() as u64 <=  1_073_741_824 {
      self.segments[self.current_segment].log_file
      .write_all(&self.log_batch)
      .expect("failed to write log batch");

      self.segments[self.current_segment].index_file
      .write_all(&self.index_batch)
      .expect("failed to write index batch")
    }

    else {
      let new_segment = Segment{
        seg_id: self.segments[self.current_segment].seg_id + 1,
        first_offset: self.segments[self.current_segment].last_offset + 1,
        last_offset: self.segments[self.current_segment].last_offset + 1,
        log_file: File::create(format!("log_file_{}", self.segments[self.current_segment].seg_id + 1)).expect("failed to create new log file"),
        index_file: File::create(format!("index_file_{}", self.segments[self.current_segment].seg_id + 1)).expect("failed to create new index file")
      };
      self.segments.push(new_segment);
      self.current_segment += 1;
      self.segments[self.current_segment].log_file
      .write_all(&self.log_batch)
      .expect("failed to write log batch");

      self.segments[self.current_segment].index_file
      .write_all(&self.index_batch)
      .expect("failed to write index batch")

    }
  }

  fn flush_batch(&mut self) {
    self.log_batch = vec![];
    self.index_batch = vec![];
  }

  fn consume(&mut self, offset: u64) -> Option<String> {
    let (segment_index, position, indexed_offset) =
    find_position(&mut self.segments, offset)?;

    let segment = &mut self.segments[segment_index];

    segment.log_file.seek(SeekFrom::Start(position)).ok()?;

    let mut records_to_skip = offset - indexed_offset;

    while records_to_skip > 0 {
        let mut offset_bytes = [0u8; 8];
        segment.log_file.read_exact(&mut offset_bytes).ok()?;

        let mut len_bytes = [0u8; 8];
        segment.log_file.read_exact(&mut len_bytes).ok()?;

        let len = u64::from_le_bytes(len_bytes);

        segment
            .log_file
            .seek(SeekFrom::Current(len as i64))
            .ok()?;

        records_to_skip -= 1;
    }

    let mut offset_bytes = [0u8; 8];
    segment.log_file.read_exact(&mut offset_bytes).ok()?;

    let mut len_bytes = [0u8; 8];
    segment.log_file.read_exact(&mut len_bytes).ok()?;

    let len = u64::from_le_bytes(len_bytes);

    let mut log_bytes = vec![0u8; len as usize];
    segment.log_file.read_exact(&mut log_bytes).ok()?;

    let (log, _) =
        bincode::serde::decode_from_slice::<Log, _>(
            &log_bytes,
            bincode::config::standard(),
        ).ok()?;

    Some(log.log)
    }
}



#[tokio::main]
async fn main() {
    let log_file = File::create("log_file_0")
        .expect("failed to create the file");
    
    let index_file = File::create("index_file_0")
        .expect("failed to create the file");

    let (tx, mut rx) = mpsc::channel::<Log>(100);
    // listener for TCP stream
    let listener = TcpListener::bind("127.0.0.1:8080")
        .await
        .unwrap();
    let segment = Segment {
        seg_id: 0,
        first_offset: 1,
        last_offset: 1,
        log_file,
        index_file
    };

    let mut logwriter = LogWriter {
        segments: vec![segment],
        current_segment: 0,
        log_batch: Vec::new(),
        index_batch: Vec::new()
    };
    let shared_state =  AppState {
        queue: tx.clone()
    };
    //task running in bg to recieve logs from the queue and send them to produce in batches of 100
    tokio::spawn(async move {
        loop {
            let mut log_count = 0;
            while log_count < 100 {
                if let Some(log) = rx.recv().await {
                    let serialized_log = bincode::serde::encode_to_vec(&log, bincode::config::standard()).unwrap();

                    let position =
                        logwriter.segments[logwriter.current_segment]
                            .log_file
                            .stream_position()
                            .unwrap()
                        + logwriter.log_batch.len() as u64;

                    let position = position.to_le_bytes();

                    let len = (serialized_log.len() as u64).to_le_bytes();

                    logwriter.segments[logwriter.current_segment].last_offset += 1;

                    let offset = logwriter.segments[logwriter.current_segment].last_offset.to_le_bytes();

                    // [offset][length][payload]
                    logwriter.log_batch.extend_from_slice(&offset);
                    logwriter.log_batch.extend_from_slice(&len);
                    logwriter.log_batch.extend_from_slice(&serialized_log);

                    if logwriter.segments[logwriter.current_segment].last_offset % 100 == 0 {
                        logwriter.index_batch.extend_from_slice(&offset);
                        logwriter.index_batch.extend_from_slice(&position);
                    }

                    log_count += 1;

                    
                }else{
                    break;
                }
            }
            logwriter.append_batch();
            logwriter.flush_batch();
        }
    });

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
                    shared_state.queue.send(log).await.expect("failed to send log to queue");
                }
            }
        };
        read_task.await;
    }

    // appending_into_the_log(log, shared_state);
}


pub fn find_position(
    segments: &mut [Segment],
    target_offset: u64,
) -> Option<(usize, u64, u64)> {
    // find the segment containing the target offset
    let segment_index = segments
        .iter()
        .position(|segment| {
            target_offset >= segment.first_offset &&
            target_offset <= segment.last_offset
        })?;

    let segment = &mut segments[segment_index];

    // each index entry:
    // [8 bytes offset][8 bytes position]
    let entry_size = 16;

    let file_size = segment.index_file.metadata().ok()?.len();
    let entry_count = file_size / entry_size;

    // No index entries yet.
    // The first log starts at position 0 and has offset 1.
    if entry_count == 0 {
        return Some((segment_index, 0, 1));
    }

    let mut low = 0;
    let mut high: u64 = entry_count;
    // binary search for largest indexed offset <= target
    while low < high {
        let mid = (low + high) / 2;

        segment
            .index_file
            .seek(SeekFrom::Start(mid * entry_size))
            .ok()?;

        let mut offset_bytes = [0u8; 8];
        let mut position_bytes = [0u8; 8];

        segment.index_file.read_exact(&mut offset_bytes).ok()?;
        segment.index_file.read_exact(&mut position_bytes).ok()?;

        let indexed_offset = u64::from_le_bytes(offset_bytes);

        if indexed_offset <= target_offset {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    if low == 0 {
        return Some((segment_index, 0, 1));
    }

    let index = low - 1;

    segment
        .index_file
        .seek(SeekFrom::Start(index * entry_size))
        .ok()?;

    let mut offset_bytes = [0u8; 8];
    let mut position_bytes = [0u8; 8];

    segment.index_file.read_exact(&mut offset_bytes).ok()?;
    segment.index_file.read_exact(&mut position_bytes).ok()?;

    let indexed_offset = u64::from_le_bytes(offset_bytes);
    let position = u64::from_le_bytes(position_bytes);
    
    Some((segment_index, position, indexed_offset))
}



#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;

    #[test]
    fn test_consume_first_log() {
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open("test_log_file")
            .unwrap();

        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open("test_index_file")
            .unwrap();

        let segment = Segment {
            seg_id: 0,
            first_offset: 1,
            last_offset: 0,
            log_file,
            index_file,
        };

        let mut writer = LogWriter {
            segments: vec![segment],
            current_segment: 0,
            log_batch: Vec::new(),
            index_batch: Vec::new(),
        };

        // Create first log
        let log = Log {
            log: "hello world".to_string(),
        };

        let serialized =
            bincode::serde::encode_to_vec(
                &log,
                bincode::config::standard()
            ).unwrap();

        let offset = 1u64.to_le_bytes();
        let len = (serialized.len() as u64).to_le_bytes();

        writer.log_batch.extend_from_slice(&offset);
        writer.log_batch.extend_from_slice(&len);
        writer.log_batch.extend_from_slice(&serialized);

        writer.segments[0].last_offset = 1;

        writer.append_batch();
        writer.flush_batch();

        let result = writer.consume(1);

        assert_eq!(result, Some("hello world".to_string()));

        std::fs::remove_file("test_log_file").unwrap();
        std::fs::remove_file("test_index_file").unwrap();
    }
}