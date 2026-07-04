# Quick Start

This guide walks you through running your first workloads on Numaflow. It is organized so you meet the simplest building block first, then layer on complexity:

1. [Prerequisites & Installation](getting-started/prerequisites-and-installation.md) - set up the tools and install Numaflow.
2. [MonoVertex](getting-started/monovertex.md) - deploy a single-vertex workload (source to sink, no Inter-Step Buffer Service required).
3. [Pipeline](getting-started/pipeline.md) - connect multiple vertices with edges, from a simple pipeline to an advanced one.
4. [What's Next](getting-started/whats-next.md) - explore more sources, sinks, user-defined functions, and examples.

## Which one should I start with?

A [MonoVertex](core-concepts/monovertex.md) is the simplest way to run Numaflow: a single vertex that reads from a source, optionally transforms the data, and writes to a sink. It needs no Inter-Step Buffer Service and has no edges, so it is the fastest thing to get running. [Start here](getting-started/monovertex.md).

A [Pipeline](core-concepts/pipeline.md) connects multiple vertices with edges and an Inter-Step Buffer Service. Use it when you need multiple processing stages, branching, joins, or windowed aggregation (reduce). Move on to this once you are comfortable with a MonoVertex.

Both run the same source, sink, transformer, and map containers, so what you learn with a MonoVertex carries directly over to pipelines.

Ready? Begin with [Prerequisites & Installation](getting-started/prerequisites-and-installation.md).
