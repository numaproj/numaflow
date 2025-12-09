/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"math"
	"time"
)

const (
	Project = "numaflow"

	// label/annotation keys.
	KeyInstance            = "numaflow.numaproj.io/instance" // instance key of the object
	KeyHash                = "numaflow.numaproj.io/hash"     // hash of the object
	KeyComponent           = "app.kubernetes.io/component"
	KeyPartOf              = "app.kubernetes.io/part-of"
	KeyManagedBy           = "app.kubernetes.io/managed-by"
	KeyAppName             = "app.kubernetes.io/name"
	KeyISBSvcName          = "numaflow.numaproj.io/isbsvc-name"
	KeyISBSvcType          = "numaflow.numaproj.io/isbsvc-type"
	KeyPipelineName        = "numaflow.numaproj.io/pipeline-name"
	KeyVertexName          = "numaflow.numaproj.io/vertex-name"
	KeyMonoVertexName      = "numaflow.numaproj.io/mono-vertex-name"
	KeyServingPipelineName = "numaflow.numaproj.io/serving-pipeline-name"
	KeyReplica             = "numaflow.numaproj.io/replica"
	KeySideInputName       = "numaflow.numaproj.io/side-input-name"
	KeyPauseTimestamp      = "numaflow.numaproj.io/pause-timestamp"
	KeyDefaultContainer    = "kubectl.kubernetes.io/default-container"
	KeyResumeStrategy      = "numaflow.numaproj.io/resume-strategy"

	DefaultISBSvcName = "default"

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
	CtrInit              = "init"
	CtrMain              = "numa"
	CtrUdf               = "udf"
	CtrUdsink            = "udsink"
	CtrFallbackUdsink    = "fb-udsink"
	CtrOnSuccessUdsink   = "ons-udsink"
	CtrUdsource          = "udsource"
	CtrUdtransformer     = "transformer"
	CtrUdSideInput       = "udsi"
	CtrInitSideInputs    = "init-side-inputs"
	CtrSideInputsWatcher = "side-inputs-synchronizer"
	CtrServing           = "serving"
	CtrMonitor           = "monitor"
	CtrUdStore           = "udstore"

	// user-defined container types
	UDContainerFunction      = "udf"
	UDContainerSink          = "udsink"
	UDContainerFallbackSink  = "fb-udsink"
	UDContainerOnSuccessSink = "ons-udsink"
	UDContainerTransformer   = "transformer"
	UDContainerSource        = "udsource"
	UDContainerSideInputs    = "udsi"
	UDContainerStore         = "udstore"

	// components
	ComponentISBSvc            = "isbsvc"
	ComponentDaemon            = "daemon"
	ComponentVertex            = "vertex"
	ComponentMonoVertex        = "mono-vertex"
	ComponentMonoVertexDaemon  = "mono-vertex-daemon"
	ComponentJob               = "job"
	ComponentSideInputManager  = "side-inputs-manager"
	ComponentUXServer          = "numaflow-ux"
	ComponentControllerManager = "controller-manager"
	ComponentPipeline          = "pipeline"
	ComponentServingServer     = "serving-server"

	// controllers
	ControllerISBSvc          = "isbsvc-controller"
	ControllerPipeline        = "pipeline-controller"
	ControllerVertex          = "vertex-controller"
	ControllerMonoVertex      = "mono-vertex-controller"
	ControllerServingPipeline = "serving-pipeline-controller"

	// ENV vars
	EnvNamespace                        = "NUMAFLOW_NAMESPACE"
	EnvPipelineName                     = "NUMAFLOW_PIPELINE_NAME"
	EnvVertexName                       = "NUMAFLOW_VERTEX_NAME"
	EnvMonoVertexName                   = "NUMAFLOW_MONO_VERTEX_NAME"
	EnvCallbackEnabled                  = "NUMAFLOW_CALLBACK_ENABLED"
	EnvCallbackURL                      = "NUMAFLOW_CALLBACK_URL"
	EnvPod                              = "NUMAFLOW_POD"
	EnvReplica                          = "NUMAFLOW_REPLICA"
	EnvVertexObject                     = "NUMAFLOW_VERTEX_OBJECT"
	EnvPipelineObject                   = "NUMAFLOW_PIPELINE_OBJECT"
	EnvMonoVertexObject                 = "NUMAFLOW_MONO_VERTEX_OBJECT"
	EnvSideInputObject                  = "NUMAFLOW_SIDE_INPUT_OBJECT"
	EnvImage                            = "NUMAFLOW_IMAGE"
	EnvImagePullPolicy                  = "NUMAFLOW_IMAGE_PULL_POLICY"
	EnvISBSvcJetStreamUser              = "NUMAFLOW_ISBSVC_JETSTREAM_USER"
	EnvISBSvcJetStreamPassword          = "NUMAFLOW_ISBSVC_JETSTREAM_PASSWORD"
	EnvISBSvcJetStreamURL               = "NUMAFLOW_ISBSVC_JETSTREAM_URL"
	EnvISBSvcJetStreamTLSEnabled        = "NUMAFLOW_ISBSVC_JETSTREAM_TLS_ENABLED"
	EnvISBSvcConfig                     = "NUMAFLOW_ISBSVC_CONFIG"
	EnvLeaderElectionDisabled           = "NUMAFLOW_LEADER_ELECTION_DISABLED"
	EnvLeaderElectionLeaseDuration      = "NUMAFLOW_LEADER_ELECTION_LEASE_DURATION"
	EnvLeaderElectionLeaseRenewDeadline = "NUMAFLOW_LEADER_ELECTION_LEASE_RENEW_DEADLINE"
	EnvLeaderElectionLeaseRenewPeriod   = "NUMAFLOW_LEADER_ELECTION_LEASE_RENEW_PERIOD"
	EnvUDContainerType                  = "NUMAFLOW_UD_CONTAINER_TYPE"
	EnvDebug                            = "NUMAFLOW_DEBUG"
	EnvPPROF                            = "NUMAFLOW_PPROF"
	EnvHealthCheckDisabled              = "NUMAFLOW_HEALTH_CHECK_DISABLED"
	EnvGRPCMaxMessageSize               = "NUMAFLOW_GRPC_MAX_MESSAGE_SIZE"
	EnvCPURequest                       = "NUMAFLOW_CPU_REQUEST"
	EnvCPULimit                         = "NUMAFLOW_CPU_LIMIT"
	EnvMemoryRequest                    = "NUMAFLOW_MEMORY_REQUEST"
	EnvMemoryLimit                      = "NUMAFLOW_MEMORY_LIMIT"
	EnvServingMinPipelineSpec           = "NUMAFLOW_SERVING_MIN_PIPELINE_SPEC"
	EnvK8sServerVersion                 = "K8S_SERVER_VERSION"
	EnvNumaflowRuntime                  = "NUMAFLOW_RUNTIME"
	EnvServingSpec                      = "NUMAFLOW_SERVING_SPEC"
	EnvServingCallbackStore             = "NUMAFLOW_SERVING_CALLBACK_STORE"
	EnvServingResponseStore             = "NUMAFLOW_SERVING_RESPONSE_STORE"
	EnvServingStatusStore               = "NUMAFLOW_SERVING_STATUS_STORE"
	EnvReadBatchSize                    = "NUMAFLOW_READ_BATCH_SIZE"
	EnvReadTimeoutMs                    = "NUMAFLOW_READ_TIMEOUT_MS"

	NumaflowRustBinary          = "/bin/numaflow-rs"
	PathVarRun                  = "/var/run/numaflow"
	VertexMetricsPort           = 2469
	VertexMetricsPortName       = "metrics"
	VertexMonitorPort           = 2470
	VertexMonitorPortName       = "monitor"
	VertexHTTPSPort             = 8443
	VertexHTTPSPortName         = "https"
	DaemonServicePort           = 4327
	MonoVertexMetricsPort       = 2469
	MonoVertexMetricsPortName   = "metrics"
	MonoVertexMonitorPort       = 2470
	MonoVertexMonitorPortName   = "monitor"
	MonoVertexDaemonServicePort = 4327
	ServingServiceHttpsPort     = 8443
	ServingServiceHttpPort      = 8090

	DefaultRequeueAfter = 10 * time.Second

	PathSideInputsMount = "/var/numaflow/side-inputs"

	// ISB
	DefaultBufferLength     = 30000
	DefaultBufferUsageLimit = 0.8
	DefaultReadBatchSize    = 500
	DefaultReadTimeout      = 1 * time.Second

	// Auto scaling
	DefaultLookbackSeconds          = 120 // Default lookback seconds for calculating avg rate and pending
	DefaultCooldownSeconds          = 90  // Default cooldown seconds after a scaling operation
	DefaultZeroReplicaSleepSeconds  = 120 // Default sleep time in seconds after scaling down to 0, before peeking
	DefaultMaxReplicas              = 50  // Default max replicas
	DefaultTargetProcessingSeconds  = 20  // Default targeted time in seconds to finish processing all the pending messages for a source
	DefaultTargetBufferAvailability = 50  // Default targeted percentage of buffer availability
	DefaultReplicasPerScale         = 2   // Default maximum replicas to be scaled up or down at once
	MaxLookbackSeconds              = 600 // Max lookback seconds for calculating avg rate and pending

	// PVC mount path for PBQ
	PathPBQMount = "/var/numaflow/pbq"

	// Pipeline health status
	PipelineStatusHealthy   = "healthy"
	PipelineStatusUnknown   = "unknown"
	PipelineStatusCritical  = "critical"
	PipelineStatusWarning   = "warning"
	PipelineStatusInactive  = "inactive"
	PipelineStatusDeleting  = "deleting"
	PipelineStatusUnhealthy = "unhealthy"

	// MonoVertex health status
	// TODO - more statuses to be added
	MonoVertexStatusHealthy   = "healthy"
	MonoVertexStatusUnhealthy = "unhealthy"
	MonoVertexStatusUnknown   = "unknown"
	MonoVertexStatusCritical  = "critical"
	MonoVertexStatusWarning   = "warning"

	// DefaultRetryInterval specifies the default initial retry duration in case of exponential backoff.
	// In case of fixed interval retry strategy, it is the default retry interval.
	// This value can be adjusted depending on the specific requirements
	// for responsiveness and system load considerations.
	DefaultRetryInterval = 1 * time.Millisecond

	// DefaultMaxRetryInterval specifies the default max retry duration.
	DefaultMaxRetryInterval = math.MaxUint32 * time.Millisecond

	// DefaultRetrySteps is defined to dictate how many times the platform should attempt to retry
	// a write operation to a sink following a failure. The value is set to math.MaxInt32 - 1,
	// effectively indicating an almost indefinite number of retries. This large default is chosen
	// to ensure that the system will try persistently to carry out the operation unless explicitly
	// configured otherwise. This approach can be useful in environments where loss of data
	// due to intermittent failures is unacceptable.
	DefaultRetrySteps = math.MaxInt32 - 1

	// DefaultFactor is the default multiplier for exponential backoff cases.
	// It is set to 1.0 to honor the default behaviour of fixed interval retries unless explicitly
	// configured otherwise.
	DefaultFactor = 1.0

	// DefaultOnFailureRetryStrategy specifies the strategy to be used when the write to a sink fails and
	// the retries count specified are exhausted.
	// Setting this to 'OnFailureRetry' means the system is configured by default
	// to retry the failed operation until successful completion.
	// This strategy argues for robustness in operations, aiming
	// to minimize the chances of data loss or failed deliveries in transient failure scenarios.
	DefaultOnFailureRetryStrategy = OnFailureRetry

	// Defeault values for readiness and liveness probes
	NumaContainerReadyzInitialDelaySeconds = 5
	NumaContainerReadyzPeriodSeconds       = 10
	NumaContainerReadyzTimeoutSeconds      = 30
	NumaContainerReadyzFailureThreshold    = 6
	NumaContainerLivezInitialDelaySeconds  = 20
	NumaContainerLivezPeriodSeconds        = 60
	NumaContainerLivezTimeoutSeconds       = 30
	NumaContainerLivezFailureThreshold     = 5
	UDContainerLivezInitialDelaySeconds    = 30
	UDContainerLivezPeriodSeconds          = 60
	UDContainerLivezTimeoutSeconds         = 30
	UDContainerLivezFailureThreshold       = 5

	//Runtime Info
	RuntimeDirVolume    = "runtime-vol"
	RuntimeDirMountPath = "/var/numaflow/runtime"
	RuntimeDirSizeLimit = 20 * 1024 * 1024

	PendingNotAvailable = int64(math.MinInt64)
)
