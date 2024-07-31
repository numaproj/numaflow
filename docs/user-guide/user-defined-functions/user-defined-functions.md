A `Pipeline` consists of multiple vertices, `Source`, `Sink` and `UDF(user-defined functions)`.

User-defined functions (UDF) is the vertex where users can run custom code to 
transform the data. Data processing in the UDF is supposed to be idempotent.

UDF runs as a sidecar container in a Vertex Pod, processes the received data. 
The communication between the main container (platform code) and the sidecar 
container (user code) is through gRPC over Unix Domain Socket.

There are two kinds of processing users can run

  - [Map](./map/map.md)
  - [Reduce](./reduce/reduce.md)
