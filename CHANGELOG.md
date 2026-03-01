## 1.0.0

- Hello World!

## 1.1.0

- Improvements on the Zeytin Engine.
- The use of adapters has been completely removed from Zeytin.

## 1.1.2

- ZeytinValue update.

## 1.2.0

- License Changed.

## 1.2.1

- Zeytin engine now supports getAllBoxes.
- The getAllBoxes function has been added to the ZeytinStorage class. You can now retrieve all your boxes.

## 1.2.2

- Readme Update.

## 2.0.0

**Major Update | Database Engine Evolution**

This update introduces a major architectural shift in the storage engine. While the data structure has been enhanced with new safety elements, a robust **backward-compatibility** layer is included. Zeytin Engine will automatically detect and migrate database files from older versions (v1.x) to the new v2 format.

> **IMPORTANT:** Although automatic migration is supported, we strongly recommend backing up your data before upgrading. This version should be thoroughly tested in your environment before production use.

### **New Features**

- **BigInt Support:** Added native support for the `BigInt` data type in `BinaryEncoder`. Large integers can now be serialized and deserialized directly without losing precision.
- **Compare-And-Swap (CAS) Support:** Introduced the `putCAS` method. This allows for atomic, conditional updates: "Update only if the current value matches the expected value," preventing data races in complex workflows.
- **Sync/Async Write Control:** Methods like `put`, `delete`, and `deleteBox` now include a `sync` parameter. You can choose between high-speed asynchronous execution (default) or guaranteed disk-flush execution (`sync: true`).

### **Performance & Optimization**

- **Smart Write Buffering:** A new in-memory `_writeBuffer` reduces disk I/O bottlenecks. Data is flushed in batches based on thresholds (100 operations or 500ms), significantly increasing throughput.
- **Fire-and-Forget Communication:** `writeFast`, `removeTagFast`, and `removeBoxFast` methods enable Isolate communication without blocking the main thread, allowing the UI or main logic to remain responsive.
- **Zero-Latency Update Checks:** `Zeytin.put` now performs update checks via the high-speed `_memoryCache` instead of querying the Isolate/Disk, drastically reducing write latency.

### **Data Security & Recovery**

- **CRC32 Checksum Protection:** Every data block now includes a 4-byte CRC32 integrity code. The engine verifies this checksum during every read operation to detect and prevent data corruption.
- **Transactional Integrity (WAL):** Buffered writes are now wrapped in `TX_START` and `TX_COMMIT` blocks. This ensures that even if a crash occurs during a write, the database remains in a consistent state.
- **Advanced Repair System:** The `_repair` logic has been completely overhauled to handle transaction logs and skip corrupted segments using CRC32 validation, ensuring maximum data recovery.
