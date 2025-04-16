# Numaflow Core Concepts Overview

Numaflow is a Kubernetes-native platform for event processing at scale, purpose-built for creating event-driven 
applications, real-time stream processing pipelines, and serving systems. It provides a set of modular and composable 
building blocks to help developers process events and serve efficiently in cloud-native environments.

The following sections introduce the core concepts that power these capabilities

* [MonoVertex](#monovertex)
* [Pipeline](#pipeline)
* [Serving](#serving)

## MonoVertex

[MonoVertex](./monovertex.md) is a lightweight way to develop event-driven applications in Numaflow. Each MonoVertex contains source, 
transformer, and sink within a single unit, making it ideal for simple event processing tasks. The entire unit scales as
one, making it a lightweight option for handling simple event processing patterns.

### Use Cases

- Stateless event transformation  
- Filtering, mapping, or enriching event data  
- Microservice-style event handling (e.g., call external APIs per event)  
- Ingesting from or writing to external systems within a single unit  


## Pipeline

[Pipeline](pipeline.md) is designed for developing real-time stream processing pipelines. It allows you to connect multiple vertices—each
representing a processing step—to handle data. Pipelines can include multiple sources and sinks, enabling integration with
diverse systems at both the input and output stages.

Each step in a pipeline can perform operations like transformation, routing, reduction, or aggregation, and is independently
scalable. This makes Pipelines ideal for building robust, modular stream processing applications.

### Use Cases
- Aggregation and windowing (e.g., sum, count, reduce over time windows)  
- Routing events based on payload or metadata  
- Multi-step data processing and enrichment  
- Scalable data pipelines for analytics and monitoring  

## Serving

[Serving](./serving.md) enables request/response interaction with streaming systems, allowing external clients to send data and 
receive processed results in real time through interfaces like REST or Server-Sent Events (SSE)

### Use Cases
- Machine learning inference (e.g., send features, receive predictions)   
- Event-driven APIs backed by stream processing logic  
- Asynchronous request/response workflows over SSE or HTTP
