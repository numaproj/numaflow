# Pipeline Operations

## Update a Pipeline

You might want to make some changes to an existing pipeline, for example, updating request CPU, or changing the minimal replicas for a vertex. Updating a pipeline is as simple as applying the new pipeline spec to the existing one. But there are some scenarios that you'd better not update the pipeline, instead, you should delete and recreate it.

The scenarios include but are not limited to:

- Topology changes such as adding or removing vertices, or updating the edges between vertices.
- Updating the [partitions](multi-partition.md) for a [keyed](../user-defined-functions/reduce/windowing/windowing.md#keyed) reduce vertex.
- Updating the user-defined container image for a vertex, while the new image can not properly handle the unprocessed data in its backlog.

To summarize, if there are unprocessed messages in the pipeline, and the new pipeline spec will change the way how the messages are processed, then you should delete and recreate the pipeline.

## Pause a Pipeline

To pause a pipeline, use the command below, it will bring the pipeline to `Paused` status, and terminate all the running vertex pods.

```bash
  kubectl patch pl my-pipeline --type=merge --patch '{"spec": {"lifecycle": {"desiredPhase": "Paused"}}}'
```

Pausing a pipeline will not cause data loss. It does not clean up the unprocessed data in the pipeline, but just terminates the running pods. When the pipeline is resumed, the pods will be restarted and continue processing the unprocessed data.

When pausing a pipeline, it will shutdown the source vertex pods first, and then wait for the other vertices to finish the backlog before terminating them. However, it will not wait forever and will terminate the pods after `pauseGracePeriodSeconds`. This is default set to 30 and can be customized by setting `spec.lifecycle.pauseGracePeriodSeconds`.

If there's a [reduce](../user-defined-functions/reduce/reduce.md) vertex in the pipeline, please make sure it uses [Persistent Volume Claim](../user-defined-functions/reduce/reduce.md#persistent-volume-claim-pvc) for storage, otherwise the data will be lost.

## Resume a Pipeline

The command below will bring the pipeline back to `Running` status.
This will resume all vertices with the same number of pods when they were running before pausing
```bash
kubectl patch pl my-pipeline \
  --type=merge \
  --patch '{"spec":{"lifecycle":{"desiredPhase":"Running"}},"metadata":{"annotations":{"numaflow.numaproj.io/resume-strategy":"fast"}}}'
```

Run the following command if you would like to resume the Pipeline vertices with the minimal number of pods (the number defined in `spec.scale.min`, otherwise 1).

```bash
kubectl patch pl my-pipeline \
  --type=merge \
  --patch '{"spec":{"lifecycle":{"desiredPhase":"Running"}},"metadata":{"annotations":{"numaflow.numaproj.io/resume-strategy":"slow"}}}'
```


## Delete a Pipeline

When deleting a pipeline, before terminating all the pods, it will try to wait for all the backlog messages that have already been ingested into the pipeline to be processed. However, it will not wait forever, if the backlog is too large, it will terminate the pods after `deletionGracePeriodSeconds`, which defaults to 30, and can be customized by setting `spec.lifecycle.deletionGracePeriodSeconds`.
