# Distributed Throttling

Distributed throttling in Numaflow is a feature that allows Numaflow to limit number of message concurrently processed by a pipeline at any
UDF. Users can configure distributed throttling at pipeline level and it will be applied to all the UDFs in the pipeline or at each vertex
level. In the following doc, throttling and rate limiting might be used interchangeably. 

## Design Details

Design details are available [here](https://github.com/numaproj/numaflow/blob/main/rust/numaflow-throttling/DESIGN.md).

## Use Cases

* Limit number of messages read from Source (useful
  for [MonoVertex](../../core-concepts/monovertex.md))
* Limit number of messages processed by a UDF (e.g., calling a critical resource like LLM)
* Limit number of messages written to Sink (e.g., calling external endpoints that does not have rate limiting)

## Assumptions

* Since Numaflow throttling is a client-side throttler, we do not have to implement realtime reconfiguration
* UDF is considered as a black-box and as a single critical resource
* All UDFs processes at the same rate, tokens are evenly distributed across all the pods in the vertex

## Configuration

To configure distributed throttling; users need to, at minimum, specify the `max` messages that the pipeline/UDF is limited to. Furthermore, 
users can also specify the `min` messages that the pipeline/UDF will be throttled to at start-up and gradually ramped-up to `max` 
messages over a `duration` amount of time. Configuration for additional features in rate limiter will be discussed in their respective sections. 

Simple spec example:
```yaml
rateLimit:
  max: 100
  min: 50
  rampUpDuration: 1s
```

## Throttling Store

Specifying an external store in the `rateLimit` config is optional but without one, throttling is done individually 
using an in-memory store, rather than distributedly across pods in a vertex. Currently redis is supported as an external store for 
keeping track of the number of processors in the pool across which throttling needs to be done distributedly.
The redis modes supported are `single` and `sentinel`.

Spec for `single` mode:
```yaml
  store:
    redisStore:
      mode: single
      url: <redis-url>
```

Spec for `sentinel` mode:
```yaml
  store:
    redisStore:
      mode: sentinel
      sentinel:
        endpoints: [<uri1>:<port1>, ...]
        master_name: <required_sentinel_svc_name>
        redis_auth: # optional
          username: <uname>
          password: <passwd>
        redis_tls: # optional
          ca_cert_secret: <io.k8s.api.core.v1.SecretKeySelector> # optional
          cert_secret: <io.k8s.api.core.v1.SecretKeySelector> # optional
          insecure_skip_verify: <bool> # optional
          key_secret: <io.k8s.api.core.v1.SecretKeySelector> # optional
        sentinel_auth: # optional, same as redis_auth
        sentinel_tls: # optional, same as redis_tls
```

## Throttling Modes

The different throttling modes that the rate limiter can be configured with, allows the user the control the behaviour of the
rate-limiter during/post ramp-up from min to max tokens.

Behaviour of a few throttling modes defined here relies on the concept of token utilization. 
The rate limiter at any instance specifies the number of messages a processor is allowed to read by providing it with 
equivalent number of tokens from the token pool based on [assumption](#assumptions) #3 above.
The token pool refers to the maximum number of messages the UDF vertex/pipeline is allowed to process at any instance.
If the processor is not able to utilize all the tokens it was provided with, for example, by not reading maximum allowable number 
of messages from ISB, then the remaining tokens are deposited back to the rate limiter, using which it calculates the token 
utilization of the processor.

### Scheduled

The number of tokens available in the token pool are increased at a fixed rate irrespective of how many tokens are used or when are the tokens requested.

Spec:
```yaml
modes:
  scheduled: {}
```

### Relaxed

If there is some traffic, then release the max possible tokens

When ramp-up is requested in this mode then the token pool size is “ramped-up” only when there is active traffic/actual calls are made to request additional tokens.
Thus, the token pool ramp-up is stalled if no calls are made for some time and resumes where it left off. 
If any calls are made to get additional tokens then the token pool size is increased irrespective of the token utilization in the previous epoch.

Relaxed mode full spec example:

```yaml
modes:
  relaxed: {}       # This is the default mode utilized when no mode is specified.
```

### OnlyIfUsed

Increase the max_ever_refilled only when the caller utilizes more tokens than the specified threshold.

Similar to relaxed mode, the token pool size increase stalls during ramp-up if no calls are being made to the rate-limiter, 
but its size also stalls when the token utilization is less than the user specified utilization threshold percentage in the previous epoch.

The token utilization is calculated using the number of tokens left over in the token pool vs the total size of the token pool:

`Token utilization % = (1 - tokens left in token pool / total size of the token pool) * 100`


`onlyIfUsed` mode full spec example:
```yaml
modes:
  onlyIfUsed:
    thresholdPercentage: 10     # at least 10% of tokens should be used before token pool is increased (default is 50)
```

### GoBackN

Unlike the previously discussed modes this mode has penalty for underutilization of tokens and penalty for gaps between
subsequent calls made to the rate limiter. The penalty is reduction in the size of the token pool by slope.
Slope is calculated as follows, using the user specified values of max tokens, min tokens and ramp-up duration:

`Slope = (max - min)/ramp-up duration`

So, similar to OnlyIfUsed mode, if the token utilization % is greater than the user specified threshold %, then the token
pool size increases by slope as usual, but if it falls below the user specified threshold %, then the token pool size is 
decreased by slope in the next epoch:

`goBackN` mode full spec example:

```yaml
modes:
  goBackN:
    thresholdPercentage: 50     # at least 50% of tokens should be used before token pool is increased, otherwise decreased
    coolDownPeriod: "5s"        # After more than 5s of no calls to the rate limiter, the token pool size is reduced
    rampDownPercentage: 50      # The % of slope by which the token pool size is reduced 
```

## Immediate ramp up during re-deployments

This is a toggleable feature that allows a pipeline that is using the rate limiter for controlled ramp-up and has already 
ramped-up from the min to max tokens to immediately ramp up to max tokens when a re-deployment is triggered instead of 
restarting from min tokens again. 

Scenario:

* Pipeline has a large ramp-up duration and a wide gap between the min and max tokens.
* The pipeline is in steady state with current throughput at max tokens.
* A re-deployment is triggered/pods are rotated.
* With `resumedRampUp` disabled, the pipeline will ramp up from min tokens again, and we'll have to wait until 
  the pipeline ramps-up to max tokens.
* With `resumedRampUp` enabled, the pipeline will directly restart from max tokens if the disruption is brief (< TTL)

Full spec example:
```yaml
rateLimit:
  resumedRampUp: false      # By default resumedRampUp is disabled
  ttl: "180s"               # The default ttl is 180s. This is the time within which, if the pipeline restarts, it will be considered as a re-deployment and 
                            # the pipeline will resume where it left off in case resumedRampUp is enabled.
```
