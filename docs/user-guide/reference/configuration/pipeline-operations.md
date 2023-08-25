# Pipeline Operations

## Pause a Pipeline

To pause a pipeline, use the command below, it will bring the pipeline to `Paused` status, and terminate all running vertex pods.

```bash
  kubectl patch pl my-pipeline --type=merge --patch '{"spec": {"lifecycle": {"desiredPhase": "Paused"}}}'
```

## Resume a Pipeline

The command below will bring the pipeline back to `Running` status.

```bash
  kubectl patch pl my-pipeline --type=merge --patch '{"spec": {"lifecycle": {"desiredPhase": "Running"}}}'
```
