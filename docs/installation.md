# Installation

Numaflow can be installed in different scopes with different approaches.

## Cluster Scope

A cluster scope installation watches and executes pipelines in all the namespaces in the cluster.

Run following command line to install latest `stable` Numaflow in cluster scope.

```shell
kubectl apply -n numaflow-system -f https://raw.githubusercontent.com/numaproj/numaflow/stable/config/install.yaml
```

If you use [kustomize](https://kustomize.io/), use `kustomization.yaml` below.

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - https://github.com/numaproj/numaflow/config/cluster-install?ref=stable # Or specify a version

namespace: numaflow-system
```

## Namespace Scope

A namespace installation only watches and executes pipelines in the namespace it is installed (typically `numaflow-system`).

Add an argument `--namespaced` to the `numaflow-controller` and `numaflow-server` deployments to achieve namespace scope installation.

```
      - args:
        - --namespaced
```

If there are multiple namespace scoped installations in one cluster, potentially there will be backward compatibility issue when any of the installation gets upgraded to a new version that has new CRD definition. To avoid this issue, we suggest to use minimal CRD definition for namespaced installation, which does not have detailed property definitions, thus no CRD changes between different versions.

```shell
# Minimal CRD
kubectl apply -f https://raw.githubusercontent.com/numaproj/numaflow/main/config/advanced-install/minimal-crds.yaml
# Controller in namespaced scope
kubectl apply -n numaflow-system -f https://github.com/numaproj/numaflow/blob/main/config/advanced-install/namespaced-controller-wo-crds.yaml
```

If you use [kustomize](https://kustomize.io/), `kustomization.yaml` looks like below.

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - https://github.com/numaproj/numaflow/config/advanced-install/minimal-crds?ref=stable # Or specify a version
  - https://github.com/numaproj/numaflow/config/advanced-install/namespaced-controller?ref=stable # Or specify a version

namespace: numaflow-system
```

## Managed Namespace Scope

A managed namespace installation watches and executes pipelines in a specific namespace.

To do managed namespace installation, besides `--namespaced`, add `--managed-namespace` and the specific namespace to the `numaflow-controller` and `numaflow-server` deployment arguments.

```
      - args:
        - --namespaced
        - --managed-namespace
        - my-namespace
```
