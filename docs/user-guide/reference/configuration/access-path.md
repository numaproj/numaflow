# UI Access Path

Currently, the base configuration will host the UI at the root `/` ie. `localhost:8443`. If a user needs to access the UI under a different path for a certain cluster, this can be achieved
with this configuration.

This can be configured in the `numaflow-server` deployment spec by adding the `--base-href` argument to the main and init containers. This will route requests from the root to the new
preferred destination. 

For example, we could port-forward the service and host at `localhost:8443/numaflow`. Note that this new access path will work with or without a trailing slash.

The following example shows how to configure the access path for the UI to `/numaflow`:

```yaml
spec:
      serviceAccountName: numaflow-server-sa
      securityContext:
        runAsNonRoot: true
        runAsUser: 9737
      volumes:
      - name: env-volume
        emptyDir: {}
      initContainers:
      - name: server-init
        image: quay.io/numaproj/numaflow:latest
        args:
        - "server-init"
        - --base-href=/numaflow # include new path here
        imagePullPolicy: Always
        volumeMounts:
        - mountPath: /opt/numaflow
          name: env-volume
      containers:
        - name: main
          image: quay.io/numaproj/numaflow:latest
          args:
          - "server"
          - --base-href=/numaflow # include new path here
          imagePullPolicy: Always
          volumeMounts:
          - mountPath: /ui/build/runtime-env.js
            name: env-volume
            subPath: runtime-env.js
          - mountPath: /ui/build/index.html
            name: env-volume
            subPath: index.html
          env:
            - name: NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
```