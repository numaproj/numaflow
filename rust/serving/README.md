# Serving

Serving is the HTTP endpoint for [Numaflow](https://numaflow.numaproj.io/) which is a distributed scalable
general-purpose
async processing platform. Unlike the normal HTTP endpoint, here we get the response from the DCG (Directed Compute
Graph) directly.

## High Level Data Movement

```mermaid
graph TD
    subgraph Stores
        CB[Callback Store<br>cb.*]
        RS[Response Store<br>rs.*]
        SS[Status Store<br>status.*]
    end

    subgraph Request Processing
        REQ[Incoming Request] -->|Register| SS
        SS -->|Track Status| PS[ProcessingStatus<br>- InProgress<br>- Completed<br>- Failed]
        REQ --> RS
        REQ -->|Fetch Response| RS
    end

    subgraph Orchestration
        MG[Message Graph] -->|Watch Callbacks| CB
        MG -->|Deregister| SS
    end

    subgraph Keys
        K1[status.id] --> SS
        K2[cb.pod-hash.id.vertex.timestamp] --> CB
        K3[rs.id] --> RS
    end
```