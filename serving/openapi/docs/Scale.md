# Scale

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**cooldown_seconds** | Option<**i64**> | Deprecated: Use scaleUpCooldownSeconds and scaleDownCooldownSeconds instead. Cooldown seconds after a scaling operation before another one. | [optional]
**disabled** | Option<**bool**> | Whether to disable autoscaling. Set to \"true\" when using Kubernetes HPA or any other 3rd party autoscaling strategies. | [optional]
**lookback_seconds** | Option<**i64**> | Lookback seconds to calculate the average pending messages and processing rate. | [optional]
**max** | Option<**i32**> | Maximum replicas. | [optional]
**min** | Option<**i32**> | Minimum replicas. | [optional]
**replicas_per_scale** | Option<**i64**> | ReplicasPerScale defines maximum replicas can be scaled up or down at once. The is use to prevent too aggressive scaling operations | [optional]
**scale_down_cooldown_seconds** | Option<**i64**> | ScaleDownCooldownSeconds defines the cooldown seconds after a scaling operation, before a follow-up scaling down. It defaults to the CooldownSeconds if not set. | [optional]
**scale_up_cooldown_seconds** | Option<**i64**> | ScaleUpCooldownSeconds defines the cooldown seconds after a scaling operation, before a follow-up scaling up. It defaults to the CooldownSeconds if not set. | [optional]
**target_buffer_availability** | Option<**i64**> | TargetBufferAvailability is used to define the target percentage of the buffer availability. A valid and meaningful value should be less than the BufferUsageLimit defined in the Edge spec (or Pipeline spec), for example, 50. It only applies to UDF and Sink vertices because only they have buffers to read. | [optional]
**target_processing_seconds** | Option<**i64**> | TargetProcessingSeconds is used to tune the aggressiveness of autoscaling for source vertices, it measures how fast you want the vertex to process all the pending messages. Typically increasing the value, which leads to lower processing rate, thus less replicas. It's only effective for source vertices. | [optional]
**zero_replica_sleep_seconds** | Option<**i64**> | After scaling down the source vertex to 0, sleep how many seconds before scaling the source vertex back up to peek. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


