# MonoVertex Operations

## Pause a MonoVertex

To pause a [MonoVertex](../../core-concepts/monovertex.md), use the command below, it will bring the MonoVertex to `Paused` status, and terminate all the running MonoVertex pods.

```bash
  kubectl patch mvtx my-mvtx --type=merge --patch '{"spec": {"lifecycle": {"desiredPhase": "Paused"}}}'
```

## Resume a MonoVertex

The command below will bring the MonoVertex back to `Running` status.

```bash
  kubectl patch mvtx my-mvtx --type=merge --patch '{"spec": {"lifecycle": {"desiredPhase": "Running"}}}'
```
