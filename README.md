WebSocket connections
        ↓
     produce()
        ↓
       Queue
        ↓
   Single Writer
        ↓
   Batch 100 logs
        ↓
      Segment
        ↓
   Sequential File Write
        ↓
       Disk