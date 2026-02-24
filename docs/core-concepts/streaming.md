# Streaming Dataflow in Numaflow

This document explains how Numaflow's Rust data-plane moves data in a streaming fashion, ensuring at-least-once semantics 
and correct [watermark](watermarks.md) propagation for reduce operations. You can read why we changed to streaming in 
our [blog post](https://blog.numaproj.io/rewriting-numaflows-data-plane-a-foundation-for-the-future-a64fd2470cf0).

## Overview

Numaflow processes data as a continuous stream rather than discrete batches (a major change in 1.6). 
The key design principles are:

1. **Always maintain active work** - Keep `batch_size` messages in-flight at all times for maximum throughput and reduce 
   tail latency.
2. **Track the smallest offset** - Never advance watermarks past unprocessed data, ensuring reduce correctness (oldest work
   yet to be completed)
3. **Backpressure via semaphores** - Prevent memory exhaustion while maximizing parallelism (throttling read if 
   processing or write is pending)

```mermaid
flowchart LR
    subgraph "Vertex N"
        ISB_R[ISB Reader] --> Processor
        Processor --> ISB_W[ISB Writer]
    end
    
    subgraph "Vertex N+1"
        ISB_R2[ISB Reader] --> Processor2[Processor]
    end
    
    ISB_W -->|ISB Edge| ISB_R2
    
    style ISB_R fill:#4a9eff,color:#fff
    style ISB_W fill:#4a9eff,color:#fff
    style Processor fill:#10b981,color:#fff
    style ISB_R2 fill:#4a9eff,color:#fff
    style Processor2 fill:#10b981,color:#fff
```

## Core Components

### ISB Reader
Reads messages from the Inter-Step Buffer (e.g., JetStream), enriches them with watermarks, and tracks them until fully 
processed, and written/forwarded to the next Vertex.

### ISB Writer  
Writes processed messages to downstream ISB streams, handles routing based on tags/conditions, and publishes watermarks.

### Tracker
Maintains a sorted map (BTreeMap) of all in-flight messages per partition. Used to:

- Track message completion for ACK/NAK
- Compute the lowest watermark among all in-flight messages
- Handle serving callbacks

### Watermark Computation
Ensures temporal correctness by tracking event-time progress and publishing watermarks to downstream vertices.

## Maintaining Active Work with Semaphores

The reader maintains a bounded number of in-flight messages using a semaphore. This provides:

1. **Constant throughput** - Always `batch_size` messages being processed
2. **Backpressure** - Prevents unbounded memory growth
3. **Graceful shutdown** - Wait for all permits to return before exiting

```mermaid
sequenceDiagram
    participant Sem as Semaphore<br/>(max_ack_pending)
    participant Reader as ISB Reader
    participant Processor as Processor
    participant Writer as ISB Writer
    
    Note over Sem: Initial permits = max_ack_pending
    
    Reader->>Sem: acquire_many(batch_size)
    Sem-->>Reader: permits granted
    Reader->>Reader: fetch messages from ISB
    
    loop For each message
        Reader->>Processor: send message (with 1 permit)
        Processor->>Writer: processed message
        Writer->>Writer: write to downstream ISB
        Writer-->>Sem: release 1 permit (on ACK)
    end
    
    Note over Sem: Permits returned = ready for next batch
```

### How It Works

1. **Reader acquires permits** - before fetching messages (`acquire_many(batch_size)`)
2. **Each message carries a permit** - split from the batch permit
3. **Permit released on ACK** - when downstream confirms receipt
4. **Backpressure automatic** - if processing is slow, reader blocks on permit acquisition

```rust
// Semaphore controls max in-flight messages
let semaphore = Arc::new(Semaphore::new(max_ack_pending));

loop {
    // Block until we can process more messages
    let permits = semaphore.acquire_many_owned(batch_size).await?;
    
    // Fetch and process batch
    let batch = self.fetch_messages(batch_size).await;
    
    for message in batch {
        // Each message gets 1 permit, released on ACK
        let permit = permits.split(1);
        self.process_message(message, permit).await;
    }
}
```

## Message Lifecycle

A message flows through several stages from read to acknowledgment:

```mermaid
flowchart TB
    subgraph "1. Read Phase"
        A[Fetch from ISB/Source] --> B[Enrich with Watermark]
        B --> C[Insert into Tracker]
        C --> D[Spawn WIP Loop]
    end
    
    subgraph "2. Process Phase"
        D --> E[Send to Processor Channel]
        E --> F[Map/Reduce/Sink Processing]
    end
    
    subgraph "3. Write Phase"
        F --> G[Route to Target Streams]
        G --> H[Write to Downstream ISB/Sink]
        H --> I[Await Write Confirmation]
    end
    
    subgraph "4. Completion Phase"
        I --> J[Send ACK/NACK via AckHandle]
        J --> K[ACK/NACK to ISB/Source]
        K --> L[Delete from Tracker]
        L --> M[Release Semaphore Permit]
    end
    
    style A fill:#4a9eff,color:#fff
    style F fill:#10b981,color:#fff
    style H fill:#f59e0b,color:#fff
    style K fill:#ef4444,color:#fff
```

### Key Points

1. **Watermark enriched at read time** - Each message gets the watermark for its offset
2. **Tracker insertion before processing** - Ensures we never lose track of a message
3. **WIP (Work-In-Progress) loop** - Periodically marks message as still being processed
4. **ACK/NACK triggers cleanup** - Removes from tracker and releases permit


## Offset Tracking for Reduce Correctness

The Tracker maintains a **BTreeMap** of offsets per partition. This sorted structure is critical for watermark correctness:

```mermaid
flowchart TB
    subgraph "Tracker State (per partition)"
        direction LR
        subgraph BTreeMap["BTreeMap&lt;Offset, TrackerEntry&gt;"]
            O1["Offset: 100<br/>WM: 1000"] 
            O2["Offset: 105<br/>WM: 1050"]
            O3["Offset: 110<br/>WM: 1100"]
            O4["Offset: 115<br/>WM: 1150"]
        end
    end
    
    O1 -->|"first_key_value()"| LW[Lowest Watermark: 1000]
    
    style O1 fill:#ef4444,color:#fff
    style LW fill:#ef4444,color:#fff
    style O2 fill:#4a9eff,color:#fff
    style O3 fill:#4a9eff,color:#fff
    style O4 fill:#4a9eff,color:#fff
```

### Why BTreeMap?

1. **Sorted by offset** - Oldest message is always `first_key_value()`
2. **O(log n) operations** - Efficient insert/delete
3. **Minimum watermark** - The first entry's watermark is the oldest in-flight

### The Lowest Watermark Guarantee

```rust
/// Returns the lowest watermark among all tracked offsets.
pub async fn lowest_watermark(&self) -> DateTime<Utc> {
    let state = self.state.read().await;
    state.entries
        .values()
        .filter_map(|partition_entries| {
            partition_entries
                .first_key_value()  // Oldest offset per partition
                .and_then(|(_, entry)| entry.watermark)
        })
        .min()  // Minimum across all partitions
}
```

### Why This Matters for Reduce

Reduce operations group data by time windows. If we wrongly advance the watermark past unprocessed data, we will
wrongly invoke close-of-book (COB) for windows and will lead to incorrect results, like:

1. **Windows close prematurely** - Data arrives after window is closed
2. **Late data is dropped** - Correctness is violated
3. **Results are wrong** - Aggregations miss data

By always publishing the **lowest watermark** among in-flight messages:

```mermaid
flowchart LR
    subgraph "In-Flight Messages"
        M1["Msg 1<br/>WM: 1000"]
        M2["Msg 2<br/>WM: 1200"]
        M3["Msg 3<br/>WM: 1100"]
    end
    
    M1 & M2 & M3 --> MIN["min() = 1000"]
    MIN --> PUB["Published WM: 1000"]
    
    subgraph "Downstream Reduce"
        W1["Window [0-1000]<br/>Safe to close (COB)"]
        W2["Window [1000-2000]<br/>Keep open"]
    end
    
    PUB --> W1 & W2
    
    style M1 fill:#ef4444,color:#fff
    style MIN fill:#ef4444,color:#fff
    style PUB fill:#ef4444,color:#fff
    style W1 fill:#10b981,color:#fff
    style W2 fill:#f59e0b,color:#fff
```

**The watermark is a promise**: "No more data with `event-time < watermark` will arrive."

## Watermark Publishing

Watermarks are published to downstream vertices via OT (Offset-Timeline) stores in JetStream KV.

### Computing the Watermark

The watermark to publish depends on the vertex type:

```mermaid
flowchart TB
    START[Compute Watermark] --> CHECK{Window Manager<br/>Configured?}
    
    CHECK -->|Yes - Reduce Vertex| WM_WIN["oldest_window_end_time - 1ms"]
    CHECK -->|No - Map/Sink| WM_TRACK["tracker.lowest_watermark()"]
    
    WM_WIN --> COMPARE["min(window_wm, latest_fetched_wm)"]
    WM_TRACK --> PUBLISH
    COMPARE --> PUBLISH[Publish to OT Store]
    
    style START fill:#4a9eff,color:#fff
    style PUBLISH fill:#10b981,color:#fff
```

### For Map/Sink Vertices

Use the tracker's lowest watermark - the minimum watermark among all in-flight messages.

### For Reduce Vertices

Use the oldest open window's `end time - 1ms`. This ensures:

- Windows aren't closed until all their data is processed
- Late data within allowed lateness is handled correctly

### Idle Watermark Handling

When no data is flowing, we still need to advance watermarks:

1. **Detect idle state** - No messages read for a period
2. **Fetch head WMB** - Get the latest watermark marker from upstream
3. **Publish idle watermark** - Allow downstream to make progress

## Streaming Flow Patterns

### Map Forwarder

```mermaid
flowchart LR
    subgraph "Map Vertex"
        R[ISB Reader] -->|ReceiverStream| M[Mapper]
        M -->|ReceiverStream| W[ISB Writer]
    end
    
    R -.->|"streaming_read()"| STREAM1[Message Stream]
    M -.->|"streaming_map()"| STREAM2[Mapped Stream]
    W -.->|"streaming_write()"| DONE[Complete]
    
    style R fill:#4a9eff,color:#fff
    style M fill:#10b981,color:#fff
    style W fill:#f59e0b,color:#fff
```

All components are connected via Tokio channels [`ReceiverStream`](https://docs.rs/tokio-stream/latest/tokio_stream/wrappers/struct.ReceiverStream.html), 
enabling true streaming without batch boundaries.

### Reduce Forwarder

```mermaid
flowchart LR
    subgraph "Reduce Vertex"
        R[ISB Reader] --> PBQ[Persistent<br/>Bounded Queue]
        PBQ --> RED[Reducer]
        PBQ --> WAL[WAL]
        RED --> W[ISB Writer]
    end

    RED -.->|"Keyed Windowing"| WINDOWS["Window 1<br/>Window 2<br/>Window N"]
    
    style R fill:#4a9eff,color:#fff
    style PBQ fill:#8b5cf6,color:#fff
    style WAL fill:#ef4444,color:#fff
    style RED fill:#10b981,color:#fff
    style W fill:#f59e0b,color:#fff
```

### Summary

The streaming architecture ensures that:

1. **Throughput is maximized** - Always `batch_size` messages in flight
2. **Memory is bounded** - Semaphore prevents unbounded growth
3. **Reduce is correct** - Watermarks never advance past unprocessed data
4. **Recovery is possible** - Tracker + WIP loop enables at-least-once or "almost" exactly-once
5. **WAL** - WAL is used for recovery in case of pod restarts for Reduce vertices.
