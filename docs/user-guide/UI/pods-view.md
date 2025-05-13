# Pods View

The **Pods View** in the Numaflow UI provides a comprehensive overview of the pods associated with a vertex. This view is designed to help users monitor the status, resource usage, and other key details of the pods in their application. It also allows users to perform actions such as filtering and inspecting individual pods.

---

## Features of the Pods View

### 1. Select a Pod by Name

- Displays a list of all pods associated with the selected vertex.
- Users can select a pod from the dropdown by its name.

---

### 2. Select a Pod by Resource

- **CPU** and **Memory** usage of the containers within pods are displayed in a hexagon heat map.
- Users can select a pod by clicking on any of these hexagons.
  - This is particularly useful when a pod's heat map is red, indicating high resource usage, and you want to inspect that specific pod.

---

### 3. Select a Container

- Allows users to select a container within a pod to view its [info](#container-info) and [logs](#container-logs).

---

### 4. Container Info

Provides detailed information about the selected container, including:

- **Name**: The name of the container.
- **Status**: The current state of the container (e.g., `Waiting`, `Running`, or `Terminated`).
- **Last Started At**: The relative age of the container since it was last (re-)started.
- **CPU**: The CPU usage compared to the requested amount.
- **Memory**: The memory usage compared to the requested amount.
- **Restart Count**: The number of times the container has been restarted.
- **Last Termination Reason**: The reason for the container's last termination (if applicable). This helps debug container crashes and restarts.
- **Last Termination Message**: A message providing details about the last termination of the container (if applicable).
- **Waiting Reason**: The reason why the container is in a waiting state (if applicable).
- **Waiting Message**: A message providing details about why the container is in a waiting state (if applicable).

---

### 5. Container Logs

- Quickly access the logs of a specific container within a pod by selecting it.
- Refer to the [Logs View](./logs.md) section for a detailed explanation of log features.

---

### 6. Pod Info

Provides detailed information about the selected pod, including:

- **Name**: The name of the pod.
- **Status**: The current phase of the pod. Possible values include:
  - `Pending`
  - `Running`
  - `Succeeded`
  - `Failed`
  - `Unknown`
    > **Note**: Learn more about pod phases [here](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase).
- **Restart Count**: The total number of restarts for all containers within the pod.
  > Since pods are recreated upon termination, this value is an aggregate of container restarts.
- **CPU**: The aggregated CPU usage of all containers within the pod compared to the requested amount.
- **Memory**: The aggregated memory usage of all containers within the pod compared to the requested amount.
- **Reason**: A brief, CamelCase message indicating why the pod is in its current state.
  - This field is populated in cases such as pod scheduling issues, eviction, termination, or node-related problems.
- **Message**: A human-readable message providing additional details about the pod's condition.
  - This field is populated in scenarios like pod scheduling issues, eviction, termination, or node-related problems.

---

### 7. Metrics Tab

- The **Pods View** includes a **Metrics Tab** located next to the Logs Tab.
- For a detailed discussion about the Metrics Tab, refer to the [Metrics](./logs.md) section.

---

## Use Cases

- **Monitoring**: Track the health and phase of all pods in your application.
- **Debugging**: Inspect logs and container details to troubleshoot issues with specific pods.
- **Resource Optimization**: Analyze CPU and memory usage to optimize resource allocation.

---
