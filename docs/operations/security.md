# Security

## Controller

Numaflow controller can be deployed in two scopes. It can be either at the Cluster level or at the Namespace 
level. When the Numaflow controller is deployed at the Namespace level, it will only have access to the Namespace
resources.

## Pipeline

### Data Movement

 - Data movement happens only within the namespace (no cross-namespaces).
 - Numaflow provides the ability to encrypt data at rest and also in transit.

### Controller and Data Plane

All communications between the controller and Numaflow pipeline components are encrypted. 
These are uni-directional read-only communications.
