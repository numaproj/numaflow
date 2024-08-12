# Numaserve

Numaserve is the serving/HTTP endpoint for [Numaflow](https://numaflow.numaproj.io/) which is a distributed scalable general-purpose
async processing platform.

## Problem

 [Numaflow](https://numaflow.numaproj.io/) being a DCG (Directed Compute Graph), there is no direct way to interact via request/response 
 protocol (in other words, the Source of Numaflow is decoupled with the Sink). There are a couple of use-cases where request/response semantics
 are required. E.g.,
 
 * Response with the status of completion (ability to say whether the processing is complete)
 * Response with output of a complex computation over the DCG directly to the callee (say, something like [kserve](https://kserve.github.io/website/latest/)).
