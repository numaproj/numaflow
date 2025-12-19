---
trigger: always_on
---

# State Layout & RocksDB (Always On)

## Rules
- Avoid million-tiny-writes patterns; prefer segmenting for table/join state.
- Use dirty-only flush; do not reintroduce full scans unless explicitly requested.
- CF separation must remain: commit_meta vs state_data (timers may be separate later).
- Any change that increases write amplification must include a mitigation plan (batching, segmentation, compaction tuning).

## Agg vs Join/table
- Agg state: compact per-key blobs (struct/protobuf)
- Join/table state: time-bucketed or shard-segmented (Arrow IPC segments)
