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

A namespace scoped installation only watches and executes pipelines in the namespace it is installed (typically `numaflow-system`).

Configure the ConfigMap `numaflow-cmd-params-config` to achieve namespace scoped installation.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: numaflow-cmd-params-config
data:
  # Whether to run in namespaced scope, defaults to false.
  namespaced: "true"
```

Another approach to do namespace scoped installation is to add an argument `--namespaced` to the `numaflow-controller` and `numaflow-server` deployments. This approach takes precedence over the ConfigMap approach.

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

To do managed namespace installation, configure the ConfigMap `numaflow-cmd-params-config` as following.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: numaflow-cmd-params-config
data:
  # Whether to run the controller and the UX server in namespaced scope, defaults to false.
  namespaced: "true"
  # The namespace that the controller and UX server watch when "namespaced" is true, defaults to the installation namespace.
  managed.namespace: numaflow-system
```

Similarly, another approach is to add `--managed-namespace` and the specific namespace to the `numaflow-controller` and `numaflow-server` deployment arguments. This approach takes precedence over the ConfigMap approach.

```
      - args:
        - --namespaced
        - --managed-namespace
        - my-namespace
```

## High Availability

By default, the Numaflow controller is installed with `Active-Passive` HA strategy enabled, which means you can run the controller with multiple replicas (defaults to 1 in the manifests).

There are 3 parameters can be tuned for the leader election mechanism of HA.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: numaflow-cmd-params-config
data:
  ### The duration that non-leader candidates will wait to force acquire leadership.
  #   This is measured against time of last observed ack. Default is 15 seconds.
  #   The configuration has to be: lease.duration > ease.renew.deadline > lease.renew.period
  controller.leader.election.lease.duration: 15s
  #
  ### The duration that the acting controlplane will retry refreshing leadership before giving up.
  #   Default is 10 seconds.
  #   The configuration has to be: lease.duration > ease.renew.deadline > lease.renew.period
  controller.leader.election.lease.renew.deadline: 10s
  ### The duration the LeaderElector clients should wait between tries of actions, which means every
  #   this period of time, it tries to renew the lease. Default is 2 seconds.
  #   The configuration has to be: lease.duration > ease.renew.deadline > lease.renew.period
  controller.leader.election.lease.renew.period: 2s
```

These 3 parameters are useful when you want to tune the frequency of leader election renewal calls to K8s API server, which are usually configured at a high priority level of [API Priority and Fairness](https://kubernetes.io/docs/concepts/cluster-administration/flow-control/).

To turn off HA, configure the ConfigMap `numaflow-cmd-params-config` as following.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: numaflow-cmd-params-config
data:
  # Whether to disable leader election for the controller, defaults to false
  controller.leader.election.disabled: "true"
```

If HA is turned off, the controller deployment should not run with multiple replicas.
