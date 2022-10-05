

# RedisConfig


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**masterName** | **String** | Only required when Sentinel is used |  [optional]
**password** | **V1SecretKeySelector** |  |  [optional]
**sentinelPassword** | **V1SecretKeySelector** |  |  [optional]
**sentinelUrl** | **String** | Sentinel URL, will be ignored if Redis URL is provided |  [optional]
**url** | **String** | Redis URL |  [optional]
**user** | **String** | Redis user |  [optional]



