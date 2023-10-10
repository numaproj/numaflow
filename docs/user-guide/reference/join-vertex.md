# Joins and Cycles

Numaflow Pipeline Edges can be defined such that multiple Vertices can forward messages to a single vertex.

### Quick Start 

Please see the following examples:

- [Join on Map Vertex](https://github.com/numaproj/numaflow/blob/main/examples/11-join-on-map.yaml)
- [Join on Reduce Vertex](https://github.com/numaproj/numaflow/blob/main/examples/11-join-on-reduce.yaml)
- [Join on Sink Vertex](https://github.com/numaproj/numaflow/blob/main/examples/11-join-on-sink.yaml)
- [Cycle to Self](https://github.com/numaproj/numaflow/blob/main/examples/10-cycle-to-self.yaml)
- [Cycle to Previous](https://github.com/numaproj/numaflow/blob/main/examples/10-cycle-to-prev.yaml)

## Why do we need JOIN

### Without JOIN
Without JOIN, Numaflow could only allow users to build [pipelines](https://numaflow.numaproj.io/core-concepts/pipeline/) where [vertices](https://numaflow.numaproj.io/core-concepts/vertex/)
could only read from previous *one* vertex. This meant that Numaflow could only support simple pipelines or tree-like pipelines. 
Supporting pipelines where you had to read from multiple sources or UDFs were cumbersome and required creating redundant
vertices.

![Simple pipeline](https://miro.medium.com/v2/resize:fit:1400/1*MAwBZ3-eOQs29fvc36XLDw.png)

![Tree-like pipeline](https://miro.medium.com/v2/resize:fit:1400/1*XXycfwWNvsTZV-cr3lomOA.png)

### With JOIN

Join vertices allow users the flexibility to read from multiple sources, process data from multiple UDFs, and even write
to a single sink. The Pipeline Spec doesn't change at all with JOIN, now you can create multiple Edges that have the 
same “To” Vertex, which would have otherwise been prohibited.

![Join Vertex](https://miro.medium.com/v2/resize:fit:1400/1*5Ct-5otqpXTAVCNW_SJnNw.png)

There is no limitation on which vertices can be joined. For instance, one can join Map or Reduce vertices as shown below:

![Directed Graph](https://miro.medium.com/v2/resize:fit:1400/1*ldVi_wtuMH4rWFd0UG91cg.png)

## Benefits

The introduction of Join Vertex allows users to eliminate redundancy in their pipelines. It supports many-to-one data 
flow without needing multiple vertices performing the same job.

## Examples

### Join on Sink Vertex

By joining the sink vertices, we now only need a single vertex responsible for sending to the data sink.

![Join on Sink Vertex](https://miro.medium.com/v2/resize:fit:1400/1*5Ct-5otqpXTAVCNW_SJnNw.png)

#### Example
[Join on Sink Vertex](https://github.com/numaproj/numaflow/blob/main/examples/11-join-on-sink.yaml)

### Join on Map Vertex

Two different Sources containing similar data that can be processed the same way can now point to a single vertex.

![Join on Map Vertex](https://miro.medium.com/v2/resize:fit:1400/1*mCXFAgbAPzyXEwJMaluxcQ.png)

#### Example

[Join on Map Vertex](https://github.com/numaproj/numaflow/blob/main/examples/11-join-on-map.yaml)

### Join on Reduce Vertex

This feature allows for efficient aggregation of data from multiple sources.

![Join on Reduce Vertex](https://miro.medium.com/v2/resize:fit:1400/1*lbuKo7wauFe5CyI4Qv0wvQ.png)

#### Example

[Join on Reduce Vertex](https://github.com/numaproj/numaflow/blob/main/examples/11-join-on-reduce.yaml)

## Cycles

A special case of a "Join" is a **Cycle** (a Vertex which can send either to itself or to a previous Vertex.) An example
use of this is a Map UDF which does some sort of reprocessing of data under certain conditions such as a transient error.

![Cycle](https://miro.medium.com/v2/resize:fit:1400/1*wYokY1wa9LhI1hKYimWiKA.png)

Cycles are permitted, except in the case that there's a Reduce Vertex at or downstream of the cycle. (This is because a 
cycle inevitably produces late data, which would get dropped by the Reduce Vertex. For this reason, cycles should be 
used sparingly.)

The following examples are of Cycles:

- [Cycle to Self](https://github.com/numaproj/numaflow/blob/main/examples/10-cycle-to-self.yaml)
- [Cycle to Previous](https://github.com/numaproj/numaflow/blob/main/examples/10-cycle-to-prev.yaml)
