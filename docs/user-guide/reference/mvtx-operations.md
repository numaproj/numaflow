# MonoVertex Operations

## Pause a MonoVertex

To pause a [MonoVertex](../../core-concepts/monovertex.md), use the command below, it will bring the MonoVertex to `Paused` state, and terminate all the running MonoVertex pods.

```bash
  kubectl patch mvtx my-mvtx --type=merge --patch '{"spec": {"lifecycle": {"desiredPhase": "Paused"}}}'
```

## Resume a MonoVertex

The command below will bring the MonoVertex back to `Running` state, resuming with the same number of pods it had prior to being paused.

```bash
  kubectl patch mvtx my-mvtx --type=merge --patch '{"spec": {"lifecycle": {"desiredPhase": "Running"}}}'
```

Run the following command if you would like to resume the MonoVertex with the minimal number of pods (the number defined in `spec.scale.min`, otherwise 1).

```bash
  kubectl patch mvtx my-mvtx --type=merge --patch '{"spec": {"lifecycle": {"desiredPhase": "Running"}, "replicas": null}}'
```
