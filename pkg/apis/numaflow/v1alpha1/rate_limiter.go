package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type RateLimit struct {
	// Max is the maximum TPS that this vertex can process give a distributed `Store` is configured. Otherwise, it will
	// be the maximum TPS for a single replica.
	Max *uint64 `json:"max,omitempty" protobuf:"varint,1,opt,name=max"`
	// Minimum TPS allowed during initial bootup. This value will be distributed across all the replicas if a distributed
	// `Store` is configured. Otherwise, it will be the minimum TPS for a single replica.
	// +kubebuilder:default=1
	Min *uint64 `json:"min,omitempty" protobuf:"varint,2,opt,name=min"`
	// RampUpDuration is the duration to reach the maximum TPS from the minimum TPS. The min unit of ramp up is 1 in
	// 1 second.
	// +kubebuilder:default= "1s"
	RampUpDuration *metav1.Duration `json:"rampUpDuration,omitempty" protobuf:"bytes,3,opt,name=rampUpDuration"`
	// Store is used to define the Distributed Store for the rate limiting. We also support in-memory store if no store
	// is configured. This means that every replica will have its own rate limit and the actual TPS will be the sum of all
	// the replicas.
	// +optional
	RateLimiterStore *RateLimiterStore `json:"store,omitempty" protobuf:"bytes,4,opt,name=store"`
	// RateLimiterModes is used to define the modes for rate limiting.
	// +optional
	RateLimiterModes *RateLimiterModes `json:"modes,omitempty" protobuf:"bytes,5,opt,name=modes"`
	// ResumedRampUp is used to enable the resume mode for rate limiting.
	//
	// This, if true, will allow the processor to
	// resume the ramp-up process from the last known state of the rate limiter, i.e., if the processor was allowed X tokens
	// before shutting down, it will be allowed X tokens again after the processor restarts.
	//
	// The resumed ramp-up process will be allowed until TTL time after the processor first deregisters with the rate limiter.
	// +optional
	// +kubebuilder:default=false
	ResumedRampUp *bool `json:"resumedRampUp,omitempty" protobuf:"bytes,6,opt,name=resumedRampUp"`
	// TTL is used to define the duration after which a pod is considered stale and removed from the pool of pods if it
	// doesn't sync with the rate limiter.
	//
	// Furthermore, if the ResumedRampUp is true, then TTL also defines the amount of time within which, if a pod
	// re-registers / registers with the same name, with the rate limiter, it will be assigned the same rate limit as
	// the previous pod with that name.
	// +optional
	// +kubebuilder:default="180s"
	TTL *metav1.Duration `json:"ttl,omitempty" protobuf:"bytes,7,opt,name=ttl"`
}

// RateLimiterModes defines the modes for rate limiting.
type RateLimiterModes struct {
	// Irrespective of the traffic, the rate limiter releases max possible tokens based on ramp-up duration.
	// +optional
	RateLimiterScheduled *RateLimiterScheduled `json:"scheduled,omitempty" protobuf:"varint,2,opt,name=scheduled"`
	// If there is some traffic, then release the max possible tokens.
	// +optional
	RateLimiterRelaxed *RateLimiterRelaxed `json:"relaxed,omitempty" protobuf:"varint,1,opt,name=relaxed"`
	// Releases additional tokens only when previously released tokens have been utilized above the configured threshold
	// +optional
	RateLimiterOnlyIfUsed *RateLimiterOnlyIfUsed `json:"onlyIfUsed,omitempty" protobuf:"bytes,3,opt,name=onlyIfUsed"`
	// Releases additional tokens only when previously released tokens have been utilized above the configured threshold
	// otherwise triggers a ramp-down. Ramp-down is also triggered when the request is made after quite a while.
	// +optional
	RateLimiterGoBackN *RateLimiterGoBackN `json:"goBackN,omitempty" protobuf:"bytes,4,opt,name=goBackN"`
}

// RateLimiterRelaxed is for the relaxed mode. It will release the max possible tokens if there is some traffic.
type RateLimiterRelaxed struct{}

// RateLimiterScheduled is for the scheduled mode.
// It will release the max possible tokens based on ramp-up duration irrespective of traffic encountered.
type RateLimiterScheduled struct{}

// RateLimiterOnlyIfUsed is for the OnlyIfUsed mode.
// Releases additional tokens only when previously released tokens have been utilized above the configured threshold
type RateLimiterOnlyIfUsed struct {
	// ThresholdPercentage specifies the minimum percentage of capacity, availed by the rate limiter,
	// that should be consumed at any instance to allow the rate limiter to unlock additional capacity.
	//
	// Defaults to 50%
	//
	// For example, given the following configuration:
	// - max = 100
	// - min = 10
	// - rampUpDuration = 10s i.e.--> slope = 10 messages/second
	// - thresholdPercentage = 50
	// at t = 0, the rate limiter will release 10 messages and at least 5 of those should be consumed to unlock
	// additional capacity of 10 messages at t = 1 to make the total capacity of 20.
	// +optional
	// +kubebuilder:default=50
	ThresholdPercentage *uint32 `json:"thresholdPercentage,omitempty" protobuf:"varint,1,opt,name=thresholdPercentage"`
}

// RateLimiterGoBackN is for the GoBackN mode.
// Releases additional tokens only when previously released tokens have been utilized above the configured threshold
// otherwise triggers a ramp-down. Ramp-down is also triggered when the request is made after quite a while.
type RateLimiterGoBackN struct {
	// CoolDownPeriod is the duration after which the rate limiter will start ramping down if the request is made after
	// the cool-down period.
	// +optional
	// +kubebuilder:default="5s"
	CoolDownPeriod *metav1.Duration `json:"coolDownPeriod,omitempty" protobuf:"bytes,1,opt,name=coolDownPeriod"`
	// RampDownStrength is the strength of the ramp-down. It is a value between 0 and 1. 0 means no ramp-down and 1 means
	// token pool is ramped down at the rate of slope=(max - min)/duration.
	// +optional
	// +kubebuilder:default=50
	RampDownPercentage *uint32 `json:"rampDownPercentage,omitempty" protobuf:"varint,2,opt,name=rampDownPercentage"`
	// ThresholdPercentage specifies the minimum percentage of capacity, availed by the rate limiter,
	// that should be consumed at any instance to allow the rate limiter to unlock additional capacity.
	// For example, given the following configuration:
	// - max = 100
	// - min = 10
	// - rampUpDuration = 10s i.e.--> slope = 10 messages/second
	// - thresholdPercentage = 50
	// at t = 0, the rate limiter will release 10 messages and at least 5 of those should be consumed to unlock
	// additional capacity of 10 messages at t = 1 to make the total capacity of 20.
	// +optional
	// +kubebuilder:default=50
	ThresholdPercentage *uint32 `json:"thresholdPercentage,omitempty" protobuf:"varint,3,opt,name=thresholdPercentage"`
}

type RateLimiterStore struct {
	// RedisStore is used to define the redis store for the rate limit.
	// +optional
	RateLimiterRedisStore *RateLimiterRedisStore `json:"redisStore,omitempty" protobuf:"bytes,1,opt,name=redisStore"`
	// InMemoryStore is used to define the in-memory store for the rate limit.
	// +optional
	RateLimiterInMemoryStore *RateLimiterInMemoryStore `json:"inMemoryStore,omitempty" protobuf:"bytes,2,opt,name=inMemoryStore"`
}

type RateLimiterInMemoryStore struct{}

type RateLimiterRedisStore struct {
	// Choose how to connect to Redis.
	// - Single: use a single URL (redis://... or rediss://...)
	// - Sentinel: discover the node via Redis Sentinel
	// +kubebuilder:validation:Enum=single;sentinel
	Mode string `json:"mode" protobuf:"bytes,1,opt,name=mode"`

	// SINGLE MODE: Full connection URL, e.g. redis://host:6379/0 or rediss://host:port/0
	// Mutually exclusive with .sentinel
	// +optional
	URL *string `json:"url,omitempty" protobuf:"bytes,2,opt,name=url"`

	// SENTINEL MODE: Settings to reach Sentinel and the selected Redis node
	// Mutually exclusive with .url
	// +optional
	Sentinel *RedisSentinelConfig `json:"sentinel,omitempty" protobuf:"bytes,3,opt,name=sentinel"`

	// COMMON: Optional DB index (default 0)
	// +optional
	// +kubebuilder:default=0
	DB *int32 `json:"db,omitempty" protobuf:"varint,4,opt,name=db"`
}

type RedisSentinelConfig struct {
	// Required Sentinel "service name" (aka master name) from sentinel.conf
	// +kubebuilder:validation:MinLength=1
	MasterName string `json:"masterName" protobuf:"bytes,1,opt,name=masterName"`

	// At least one Sentinel endpoint; 2–3 recommended. Use host:port pairs.
	// Example: ["sentinel-0.redis.svc:26379", "sentinel-1.redis.svc:26379"]
	// +kubebuilder:validation:MinItems=1
	Endpoints []string `json:"endpoints" protobuf:"bytes,2,rep,name=endpoints"`

	// Auth to talk to the Sentinel daemons (control-plane). Optional.
	// +optional
	SentinelAuth *RedisAuth `json:"sentinelAuth,omitempty" protobuf:"bytes,4,opt,name=sentinelAuth"`

	// Auth to talk to the Redis data nodes (data-plane). Optional.
	// +optional
	RedisAuth *RedisAuth `json:"redisAuth,omitempty" protobuf:"bytes,5,opt,name=redisAuth"`

	// TLS for Sentinel connections (if your Sentinels expose TLS).
	// +optional
	SentinelTLS *TLS `json:"sentinelTLS,omitempty" protobuf:"bytes,6,opt,name=sentinelTLS"`

	// TLS for Redis data nodes (redis). Often enabled even if Sentinel is plaintext.
	// +optional
	RedisTLS *TLS `json:"redisTLS,omitempty" protobuf:"bytes,7,opt,name=redisTLS"`
}

type RedisAuth struct {
	// For Redis 6+ ACLs. If Username omitted, password-only is also supported.
	// +optional
	Username *corev1.SecretKeySelector `json:"username,omitempty" protobuf:"bytes,1,opt,name=username"`
	// +optional
	Password *corev1.SecretKeySelector `json:"password,omitempty" protobuf:"bytes,2,opt,name=password"`
}
