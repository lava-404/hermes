#[cfg(test)]
mod tests {
    use super::*;

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