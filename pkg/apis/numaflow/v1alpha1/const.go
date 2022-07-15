package v1alpha1

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
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
	KeyMetaID = "x-numaflow-id"

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
	CtrMain   = "main"
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
	EnvNamespace                   = "NUMAFLOW_NAMESPACE"
	EnvPipelineName                = "NUMAFLOW_PIPELINE_NAME"
	EnvVertexName                  = "NUMAFLOW_VERTEX_NAME"
	EnvPod                         = "NUMAFLOW_POD"
	EnvReplica                     = "NUMAFLOW_REPLICA"
	EnvVertexObject                = "NUMAFLOW_VERTEX_OBJECT"
	EnvPipelineObject              = "NUMAFLOW_PIPELINE_OBJECT"
	EnvImage                       = "NUMAFLOW_IMAGE"
	EnvImagePullPolicy             = "NUMAFLOW_IMAGE_PULL_POLICY"
	EnvUDFContentType              = "NUMAFLOW_UDF_CONTENT_TYPE"
	EnvUDSinkContentType           = "NUMAFLOW_UDSINK_CONTENT_TYPE" // Content-Type for the user defined sinks
	EnvISBSvcRedisSentinelURL      = "NUMAFLOW_ISBSVC_REDIS_SENTINEL_URL"
	EnvISBSvcSentinelMaster        = "NUMAFLOW_ISBSVC_REDIS_SENTINEL_MASTER"
	EnvISBSvcRedisURL              = "NUMAFLOW_ISBSVC_REDIS_URL"
	EnvISBSvcRedisUser             = "NUMAFLOW_ISBSVC_REDIS_USER"
	EnvISBSvcRedisPassword         = "NUMAFLOW_ISBSVC_REDIS_PASSWORD"
	EnvISBSvcRedisSentinelPassword = "NUMAFLOW_ISBSVC_REDIS_SENTINEL_PASSWORD"
	EnvISBSvcJetStreamUser         = "NUMAFLOW_ISBSVC_JETSTREAM_USER"
	EnvISBSvcJetStreamPassword     = "NUMAFLOW_ISBSVC_JETSTREAM_PASSWORD"
	EnvISBSvcJetStreamURL          = "NUMAFLOW_ISBSVC_JETSTREAM_URL"
	EnvISBSvcJetStreamTLSEnabled   = "NUMAFLOW_ISBSVC_JETSTREAM_TLS_ENABLED"
	EnvISBSvcConfig                = "NUMAFLOW_ISBSVC_CONFIG"
	EnvDebug                       = "NUMAFLOW_DEBUG"

	// Watermark
	EnvWatermarkOn = "NUMAFLOW_WATERMARK_ON"

	PathVarRun            = "/var/run/numaflow"
	VertexMetricsPort     = 2469
	VertexMetricsPortName = "metrics"
	VertexHTTPSPort       = 8443
	VertexHTTPSPortName   = "https"
	DaemonServicePort     = 4327

	DefaultRequeueAfter = 10 * time.Second

	DefaultBufferLength     = 50000
	DefaultBufferUsageLimit = 0.8

	UDFApplierMessageKey = "x-numa-message-key" // The key in the UDF applier HTTP header used to pass the map-reduce key

	// processing rate and pending prometheus metrics and labels
	MetricPipelineLabel   = "pipeline"
	MetricVertexLabel     = "vertex"
	MetricPeriodLabel     = "period"
	VertexProcessingRate  = "vertex_processing_rate"
	VertexPendingMessages = "vertex_pending_messages"
)

type ContentType string

const (
	JsonType    ContentType = "application/json"
	MsgPackType ContentType = "application/msgpack"
)

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
