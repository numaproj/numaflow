package v1alpha1

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	Project = "numaflow"

	// label/annotation keys.
	KeyHash         = "numaflow.numaproj.io/hash" // hash of the object
	KeyComponent    = "app.kubernetes.io/component"
	KeyPartOf       = "app.kubernetes.io/part-of"
	KeyManagedBy    = "app.kubernetes.io/managed-by"
	KeyAppName      = "app.kubernetes.io/name"
	KeyISBSvcName   = "numaflow.numaproj.io/isbsvc-name"
	KeyISBSvcType   = "numaflow.numaproj.io/isbsvc-type"
	KeyPipelineName = "numaflow.numaproj.io/pipeline-name"
	KeyVertexName   = "numaflow.numaproj.io/vertex-name"
	KeyReplica      = "numaflow.numaproj.io/replica"

	// ID key in the header of sources like http
	KeyMetaID        = "x-numaflow-id"
	KeyMetaEventTime = "x-numaflow-event-time"

	DefaultISBSvcName = "default"

	DefaultRedisSentinelMasterName = "mymaster"
	RedisAuthSecretKey             = "redis-password" // Redis password secret key

	JetStreamServerSecretAuthKey         = "auth"                 // key for auth server secret
	JetStreamServerSecretEncryptionKey   = "encryption"           // key for encryption server secret
	JetStreamServerPrivateKeyKey         = "private-key"          // key for server private key
	JetStreamServerCertKey               = "cert"                 // key for server TLS certificate
	JetStreamServerCACertKey             = "ca-cert"              // key for server CA certificate
	JetStreamClusterPrivateKeyKey        = "cluster-private-key"  // key for server private key
	JetStreamClusterCertKey              = "cluster-cert"         // key for server TLS certificate
	JetStreamClusterCACertKey            = "cluster-ca-cert"      // key for server CA certificate
	JetStreamClientAuthSecretUserKey     = "client-auth-user"     // key for client auth user secret
	JetStreamClientAuthSecretPasswordKey = "client-auth-password" // key for client auth password secret
	JetStreamConfigMapKey                = "nats-js"              // key for nats-js.conf in the configmap

	// container names.
	CtrInit   = "init"
	CtrMain   = "numa"
	CtrUdf    = "udf"
	CtrUdsink = "udsink"

	// components
	ComponentISBSvc = "isbsvc"
	ComponentDaemon = "daemon"
	ComponentVertex = "vertex"

	// controllers
	ControllerISBSvc   = "isbsvc-controller"
	ControllerPipeline = "pipeline-controller"
	ControllerVertex   = "vertex-controller"
	ControllerWatchdog = "watchdog"

	// ENV vars
	EnvNamespace                      = "NUMAFLOW_NAMESPACE"
	EnvPipelineName                   = "NUMAFLOW_PIPELINE_NAME"
	EnvVertexName                     = "NUMAFLOW_VERTEX_NAME"
	EnvPod                            = "NUMAFLOW_POD"
	EnvReplica                        = "NUMAFLOW_REPLICA"
	EnvVertexObject                   = "NUMAFLOW_VERTEX_OBJECT"
	EnvPipelineObject                 = "NUMAFLOW_PIPELINE_OBJECT"
	EnvImage                          = "NUMAFLOW_IMAGE"
	EnvImagePullPolicy                = "NUMAFLOW_IMAGE_PULL_POLICY"
	EnvISBSvcRedisSentinelURL         = "NUMAFLOW_ISBSVC_REDIS_SENTINEL_URL"
	EnvISBSvcSentinelMaster           = "NUMAFLOW_ISBSVC_REDIS_SENTINEL_MASTER"
	EnvISBSvcRedisURL                 = "NUMAFLOW_ISBSVC_REDIS_URL"
	EnvISBSvcRedisUser                = "NUMAFLOW_ISBSVC_REDIS_USER"
	EnvISBSvcRedisPassword            = "NUMAFLOW_ISBSVC_REDIS_PASSWORD"
	EnvISBSvcRedisSentinelPassword    = "NUMAFLOW_ISBSVC_REDIS_SENTINEL_PASSWORD"
	EnvISBSvcRedisClusterMaxRedirects = "NUMAFLOW_ISBSVC_REDIS_CLUSTER_MAX_REDIRECTS"
	EnvISBSvcJetStreamUser            = "NUMAFLOW_ISBSVC_JETSTREAM_USER"
	EnvISBSvcJetStreamPassword        = "NUMAFLOW_ISBSVC_JETSTREAM_PASSWORD"
	EnvISBSvcJetStreamURL             = "NUMAFLOW_ISBSVC_JETSTREAM_URL"
	EnvISBSvcJetStreamTLSEnabled      = "NUMAFLOW_ISBSVC_JETSTREAM_TLS_ENABLED"
	EnvISBSvcConfig                   = "NUMAFLOW_ISBSVC_CONFIG"
	EnvDebug                          = "NUMAFLOW_DEBUG"

	// Watermark
	EnvWatermarkDisabled = "NUMAFLOW_WATERMARK_DISABLED"
	EnvWatermarkMaxDelay = "NUMAFLOW_WATERMARK_MAX_DELAY"

	PathVarRun            = "/var/run/numaflow"
	VertexMetricsPort     = 2469
	VertexMetricsPortName = "metrics"
	VertexHTTPSPort       = 8443
	VertexHTTPSPortName   = "https"
	DaemonServicePort     = 4327

	DefaultRequeueAfter = 10 * time.Second

	// ISB
	DefaultBufferLength     = 50000
	DefaultBufferUsageLimit = 0.8
	DefaultReadBatchSize    = 500

	// Auto scaling
	DefaultLookbackSeconds         = 180 // Default lookback seconds for calculating avg rate and pending
	DefaultCooldownSeconds         = 90  // Default cooldown seconds after a scaling operation
	DefaultZeroReplicaSleepSeconds = 180 // Default sleep time in seconds after scaling down to 0, before peeking
	DefaultMaxReplicas             = 50  // Default max replicas
	DefaultTargetProcessingSeconds = 20  // Default targeted time in seconds to finish processing all the pending messages for a source
	DefaultTargetBufferUsage       = 50  // Default targeted percentage of balanced buffer usage
	DefaultReplicasPerScale        = 2   // Default maximum replicas to be scaled up or down at once

	// Default persistent buffer queue options
	DefaultPBQChannelBufferSize = 10000           // Default channel size in int
	DefaultPBQReadTimeout       = 1 * time.Second // Default read timeout for pbq
	DefaultPBQReadBatchSize     = 100             // Default read batch size for pbq

	// Default persistent store options
	DefaultStoreSyncDuration  = 2 * time.Second // Default sync duration for pbq
	DefaultStoreType          = NoOpType        // Default store type
	DefaultStoreSize          = 1000000         // Default persistent store size
	DefaultStoreMaxBufferSize = 100000          // Default buffer size for pbq in bytes

	// Default window options
	DefaultWindowType     = FixedType
	DefaultWindowDuration = 0
)

// PBQ store's backend type.
type StoreType string

const (
	InMemoryType   StoreType = "in-memory"
	FileSystemType StoreType = "file-system"
	NoOpType       StoreType = "no-op"
)

func (st StoreType) String() string {
	switch st {
	case InMemoryType:
		return string(InMemoryType)
	case FileSystemType:
		return string(FileSystemType)
	case NoOpType:
		return string(NoOpType)
	default:
		return "unknownStoreType"
	}
}

type WindowType string

const (
	FixedType   WindowType = "fixed"
	SlidingType WindowType = "sliding"
	SessionType WindowType = "session"
)

func (wt WindowType) String() string {
	switch wt {
	case FixedType:
		return string(FixedType)
	case SlidingType:
		return string(SlidingType)
	case SessionType:
		return string(SessionType)
	default:
		return "unknownWindowType"
	}
}

var (
	MessageKeyDrop = fmt.Sprintf("%U__DROP__", '\\') // U+005C__DROP__
	MessageKeyAll  = fmt.Sprintf("%U__ALL__", '\\')  // U+005C__ALL__

	// the standard resources used by the `init` and `main`containers.
	standardResources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			"cpu":    resource.MustParse("100m"),
			"memory": resource.MustParse("128Mi"),
		},
	}
)
