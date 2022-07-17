# Installation

Numaflow can be installed in different scopes.

## Cluster Scope

A cluster scope installation watches and executes pipelines in all the namespaces of the cluster.

## Namespace Scope

A namespace installation only watches and executes pipelines in the namespace it installed (typically `numaflow-system`).

Add an argument `--namespaced` to the `numaflow-controller` deployment to achieve namespace scope installation.

```
      - args:
        - --namespaced
```

## Managed Namespace Scope

A managed namespace installation watches and executes pipelines in a specific namespace.

To do managed namespace installation, besides `--namespaced`, add `--managed-namespace` and the specific namespace to the `numaflow-controller` deployment arguments.

```
      - args:
        - --namespaced
        - --managed-namespace
        - my-namespace
```
