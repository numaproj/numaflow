# Access Path

There is an option to change the UI access path for the Numaflow server. 

This can be configured in the `numaflow-server` deployment spec by adding the `--base-href` argument to the main and init containers.

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