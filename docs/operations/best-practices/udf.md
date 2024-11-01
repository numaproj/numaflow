# User Defined Functions

Few guidelines and tips to help with common use cases and FAQs for UDFs

## High latency operations in UDF

If running a user code which has very high latency or high processing time

1. Increase the `lookbackSeconds` for the autoscaling - This dictates how many seconds to look back for vertex average 
processing rate (tps) and pending messages calculation, defaults to `120`. In case of tasks taking longer than 
this duration, you need to increase`lookbackSeconds` so that the calculated average rate and pending messages won't 
be 0 during the silent period. Refer [Autoscaling](https://numaflow.numaproj.io/user-guide/reference/autoscaling/) for more information  
```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  vertices:
    - name: my-vertex
      scale:
        lookbackSeconds: 120 # Optional, defaults to 120.
        ....
```

