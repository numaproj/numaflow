# UI Access Path

By default, Numaflow UI server will host the service at the root `/` ie. `localhost:8443`. If a user needs to access the
UI server under a different path, this can be achieved with following configuration. This is useful when the UI is hosted
behind a reverse proxy or ingress controller that requires a specific path.

Configure `server.base.href` in the ConfigMap `numaflow-cmd-params-config`.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: numaflow-cmd-params-config
data:
  ### Base href for Numaflow UI server, defaults to '/'.
  server.base.href: "/app"
```

The configuration above will host the service at `localhost:8443/app`. Note that this new access path will work with or
without a trailing slash.
