# Side Inputs

Side Inputs allow the user defined functions (including UDF, UDSink, Transformer, etc.) to access slow updated data or configuration (such as database, file system, etc.) without needing to load it during each message processing. Side Inputs are read-only and can be used in both batch and streaming jobs.

## Requirements

- The Side Inputs should be programmable with any language.
- The Side Inputs should be updated centralized (for a pipeline), and be able to broadcast to each of the vertex pods in an efficient manner.
- The Side Inputs update could be based on a configurable interval.

## Assumptions

- Size of a Side Input data could be up to 1MB.
- The Side Inputs data is updated at a low frequency (minutes level).
- As a platform, Numaflow has no idea about the data format of the Side Inputs, instead, the pipeline owner (programmer) is responsible for parsing the data.

## Design Proposal

### Data Format

Numaflow processes the Side Inputs data as bytes array, thus there’s no data format requirement for it, the pipeline developers are supposed to parse the Side Inputs data from bytes array to any format they expect.

### Architecture

There will be the following components introduced when a pipeline has Side Inputs enabled.

- **A Side Inputs Manager** - a service for Side Inputs data updating.
- **A Side Inputs watcher sidecar** - a container enabled for each of the vertex pods to receive updated Side Inputs.
- **Side Inputs data store** - a data store to store the latest Side Inputs data.

## Open Issues

- To support multiple ways to trigger Side Inputs updating other than cron only?
- Event based side inputs where the changes are coming via a stream.
