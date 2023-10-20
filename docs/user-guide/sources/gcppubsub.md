# GCP Pub/Sub Source

A `pubsub` source is used to ingest the messages from a GCP Pub/Sub topic.

```yaml
spec:
  vertices:
    - name: input
      source:
        gcpPubSub:
          # (optional) GCP project ID for the subscription.
          # Required if you run numaflow outside of GKE/GCE.
          # (otherwise, the default value is its project)
          projectID: project-XXXXX
          # (optional) GCP project ID for the topic.
          # By default, it is same as ProjectID.
          # topicProjectID: "project-id"
          # (optional) Topic to which the subscription should belongs.
          # Required if you want the eventsource to create a new subscription.
          # If you specify this field along with an existing subscription,
          # it will be verified whether it actually belongs to the specified topic.
          topic: test
          # (optional) ID of subscription.
          # Required if you use existing subscription.
          # The default value will be auto generated hash based on this eventsource setting, so the subscription
          # might be recreated every time you update the setting, which has a possibility of event loss.
          subscriptionID: test-sub
          # (optional) Refers to a k8s secret which contains the credential JSON to access pubsub.
          # If it is missing, it implicitly uses Workload Identity.
          credentialSecret:
            name: my-secret
            key: secret-key
```
