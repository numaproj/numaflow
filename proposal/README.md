# Summary

# Motivation
With advances in sensing, networking, and AI processing, modern ICT infrastructures are facing rapidly increasing demands for large-scale data processing. As a result, the power consumption required to handle these large volumes of data has become a serious concern. In other words, today’s infrastructures must simultaneously improve processing performance while reducing energy consumption.

For dataflow-based platforms such as Numaflow, using hardware accelerators for individual tasks (pods) can significantly improve performance per watt. We expect that, in the near future, most tasks in an application will run on specialized accelerators. 

While accelerators reduce task execution time, processing can be bottlenecked if data transfer speeds between tasks do not improve accordingly. Therefore, as data processing performance increases, the performance requirements for data communication to support large-scale data transfers also become more demanding.

Therefore, we consider introducing a high-speed communication method with low transfer overhead, such as GPUDirect RDMA, for inter-vertex communication. To achieve this, the following elements are required.

1. GPUDirect RDMA, which enables direct device-to-device communication between GPUs, requires RDMA-capable NICs. In other words, it is necessary to introduce a high-speed network by assigning a second NIC for RDMA to each pod, separate from the default network.
2. To enable direct communication using GPUDirect RDMA, a method is required to specify the peer device based on its IP address on a high-speed network.

Therefore, to realize the ICT infrastructure required in the near future, we propose new approaches for Numaflow regarding (1) the type of network and (2) the data communication method used between vertices.

**the type of network used between vertices**

Currently, pod-to-pod communication in Kubernetes clusters is handled via the default network. However, to support use cases that process large volumes of data, such as LLMs, there is a growing demand for high-bandwidth networks in Kubernetes clusters.

Accordingly, within the Kubernetes community’s Device Management Working Group, discussions are underway to enable the construction of high-speed networks (MultiNetwork) by assigning a second NIC to pods using the DRA feature proposed by the WG.

Based on this trend, we also aim to introduce MultiNetwork into Numaflow and enable MultiNetwork to be specified as the network used for communication between vertices.

**the data communication method used between vertices**

In the current Numaflow architecture, data communication is performed via the InterStepBufferService (ISBSVC) resource from the main container, which is separate from the UDF container that executes the actual processing.

In contrast, GPUDirect RDMA enables direct device-to-device communication. Therefore, instead of using ISBSVC and the main container, data is sent directly from a UDF container to the next UDF container that acts as the downstream processing entity.

Accordingly, to enable GPUDirect RDMA in Numaflow, we introduce a mechanism that provides the necessary connection information—such as IP address, port, and GPU ID—required for direct device-to-device communication using GPUDirect RDMA.


# Design Detail

> [!WARNING]
> With a view to supporting InterStepBufferService (ISBSVC) on MultiNetwork in the future, the resource specification is designed with potential ISBSVC extensions in mind.
>
>However, the extension of ISBSVC itself is outside the scope of this proposal.

## Pipeline Architecture

An example pipeline based on the Video Inference System is presented below.
- This example is equivalent to the pipeline manifest shown below.
- In this pipeline, only Map UDFs are considered, and Reduce UDFs are out of scope.

![Pipeline Architecture](./assets/pipeline_architecture.drawio.png)

> [!IMPORTANT] Basic Architecture Overview
> - A Vertex (CR) is the fundamental unit of a pipeline.
> - Each Vertex consists of one or more pods. Each pod contains a container responsible for data processing and a main container responsible for data communication.
> 
> - Vertex scaling is classified into two types:
>   - Horizontal scaling, achieved by increasing or decreasing the number of pods.
>   - Vertical scaling, achieved by increasing or decreasing the number of Numaflow processors within the main container, which handle the actual communication processing.

## Resource Specification
### (New) numaNetwork
#### Overview
- The numaNetwork is a Custom Resource (CR) that defines the network configuration itself, which users design on MultiNetwork.
  - While users can define multiple numaNetwork resources, it is not feasible to allow unlimited creation. Therefore, the CR should be designed to support configurable limits, such as an upper bound on the number of resources, in the future.
- This CR can be referenced from the InterStepBufferService (ISBSVC) manifest and the edge field of the Pipeline manifest.
  - Each edge can specify exactly one numaNetwork; however, the same or different numaNetwork resources may be specified on a per-edge basis.
  - In addition, the same numaNetwork can be shared across edges belonging to different pipelines.
- The numaNetwork information is referenced during the processing flows of the InterStepBufferService and Pipeline controllers, and is used to create the required fields.

#### Necessity
Regardless of whether a Vertex uses ISB-based communication or Direct Transfer, when communicating over MultiNetwork, users must design the network so that all Vertices can reach each other. Based on this design, ResourceClaims must be specified for each Vertex, which results in significant operational complexity.

To address this issue, we define the MultiNetwork used by a Pipeline and associate it with Vertices and ISBs. This approach unifies the network configuration used by each Pipeline and simplifies network management.

![Network Architecture](./assets/network_architecture)

#### Specification
```
apiVersion: numaflow.numaproj.io/v1alpha1
kind: numaNetwork
metadata:
  name: pipeline1-multi-network
spec:
  ipRange: 192.168.10.0/24               # ★1
  ethernetSpeed: 100                     # ★2
  vlanTag: 10                            # ★3
```

| Parameter Path | Description |
| :- | :- |
| ipRange<br>(★1) | The IP range assigned to the secondary NIC by the dranet IPAM tool<br><br> **IMPORTANT**: Workflow Overview<br>The ipRange parameter is converted into dranet-specific fields in ResourceClaims. This parameter is then consumed by dranet, where an internal IPAM tool allocates IP addresses and assigns NICs accordingly. |
| ipRange<br>(★2) | The maximum Ethernet link speed (Gb/s) used along the physical network path.<br>- Since software cannot obtain the maximum capacity of the physical Ethernet cabling in use, this value is intended for manifest authors to record network configuration information. |
| vlanTag<br>(★3) | The VLAN tag value assigned to data belonging to a numaNetwork. |

### Pipeline
#### Specification
```
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: ...
spec:
  vertices:
    - name: in
      ...
    - name: filter-resize
      ...
      udf:
        container:
          ...
          resources:
            claims:
              - name: gpu
              - name: secondNIC
      resourceClaims:
        - name: gpu
          resourceClaimTemplateName: A100
    - name: inference
      ...
      udf:
        container:
          ...
          resources:
            claims:
              - name: gpu
              - name: secondNIC
      resourceClaims:
        - name: gpu
          resourceClaimTemplateName: A100
      ...
    - name: out
      ...
          
  edges:
    - from: in
      to: filter-resize
    - from: filter-resize
      to: inference
      numaNetwork:
        name: pipeline1-multi-network  # ★1
        connectionType: direct         # ★2
    - from: inference
      to: out
      numaNetwork:
        name: pipeline1-multi-network  # ★1
        connectionType: multi-isbsvc   # ★2
```

| Parameter Path | Description |
| :-- | :-- |
| edges[].numaNetwork.name<br>(★1) | Specifies the name of the numaNetwork used for communication between Vertices.<br> - If this parameter is omitted, the communication falls back to the default behavior of Numaflow, in which communication is performed via ISBSVC on the DefaultNetwork. |
| edges[].numaNetwork.connectionType<br>(★2) | Specifies the communication method used for connections between Vertices.<br> - direct: Performs direct UDF-to-UDF communication using the numaNetwork specified by name.<br> - multi-isbsvc: Performs communication via an InterStepBufferService (ISBSVC) associated with the numaNetwork specified by name.

## Workflow
### Preparation for Assigning a Second NIC to Pipeline resources
1. Partitioning the NIC into VFs
2. Deploying ResourceClaims (Templates) and DeviceClasses

### Deployment Sequence of Pipeline resources
[Deployment Sequence](./assets/deployment_sequence.drawio.png)

1. Deploy the numaNetwork and store its resource information in etcd.
2. Deploy the Pipeline.
	1. The Pipeline controller accesses the API server and retrieves the numaNetwork information.
	2. The Pipeline controller creates fields in the Pipeline resource to assign a secondary NIC to pods within each Vertex.
	   - These fields are eventually treated as pod fields.

### Assignment of a Secondary NIC During Resource Deployment

The following behavior occurs after the numaNetwork information has been propagated to the resourceClaims fields of pods during the deployment of ISBSVC and Pipeline resources:

1. A DRA driver for NICs is specified via the DeviceClass referenced by the ResourceClaims.
2. IP addresses are assigned to pods using the IPAM tool specified in the NIC DRA driver.
3. An external controller registers the pod name and assigned IP address with CoreDNS.

> [!IMPORTANT]
> This external controller does not exist as a component in the current Numaflow architecture.
It may be newly implemented or an existing controller may be reused; however, the critical requirement is the presence of a component responsible for performing this registration.

### Direct Communication Processing During Application Execution
- Premise: 
  - Within a Vertex, the UDF container is aware of the destination Vertex name, which is equivalent to a Service in a typical microservices architecture.
  - This processing is assumed to be performed using a library provided by the Numaflow SDK.

1. The interface function accepts the data to be sent and the required buffer size as its arguments.
2. The IP addresses of candidate destination pods are retrieved using the destination Vertex name obtained from environment variables.
  - In languages such as Python, this can be achieved using functions like socket.getaddrinfo. 
  - Name resolution follows the standard OS-level procedure; therefore, no special handling is required as long as the library function relies on the operating system’s name resolution mechanism.
    - Specifically, the function queries the OS resolver, which refers to /etc/resolv.conf and sends a request to the DNS server specified there (CoreDNS). CoreDNS then queries the Kubernetes API and returns a list of pod IP addresses.
    - Since the resolved destination candidates are expected to be stored in an internal cache (within the container’s OS space), this lookup is assumed to occur only once in most cases.
3. Data is then sent to the destination pods using a selection strategy such as round-robin.
