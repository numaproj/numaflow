# Auto Scaling

Vertex auto scaling can be done by using `scale` as following example:

```yaml
spec:
  vertices:
    - name: my-vertex
      scale:
        min: 2 # Minimal replicas
        max: 8 # Maximum replicas
      udf:
        container:
          image: my-python-udf-example:latest
```
