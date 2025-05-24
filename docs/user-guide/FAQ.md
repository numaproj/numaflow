# Frequently Asked Questions

Welcome to the Numaflow FAQ! Here you'll find answers to common questions about getting started, compatibility, troubleshooting, and more.

---

### 1. How do I get started with Numaflow?

To get started, follow the [Quickstart Guide](../quick-start.md). It provides step-by-step instructions for setting up a basic Numaflow pipeline.

---

### 2. How can I check SDK compatibility with Numaflow versions?

Refer to the compatibility matrix [here](../user-guide/sdks/compatibility.md) to ensure your SDK version works with your Numaflow deployment.

---

### 3. Where can I learn about the latest releases of Numaflow?

-   Visit the [Numaflow Releases page](https://github.com/numaproj/numaflow/releases) for the latest updates.
-   Join our [Slack channel](https://join.slack.com/t/numaproj/shared_invite/zt-19svuv47m-YKHhsQ~~KK9mBv1E7pNzfg) to stay up to date with announcements and community discussions.

---

### 4. I see `Server Info File not ready` log in the numa container. What does this mean?

This message indicates that the server has not started yet. Common reasons include:

-   The server was not started correctly in the `udcontainer`.
-   The server is still initializing (for example, the application code is downloading cache files or performing setup tasks).

### 5. What is `partitions` in Numaflow?

The field `partitions` is the number of streams between the vertices. Larger the parition number, higher the TPS that vertex can take. Note that partitions are owned by the vertex reading the data, to create a
multi-partitioed edge we need to configure the vertex reading the data to have multiple partitions.

If you have additional questions, please refer to the [documentation](../README.md) or reach out on [Slack](https://join.slack.com/t/numaproj/shared_invite/zt-19svuv47m-YKHhsQ~~KK9mBv1E7pNzfg)!
