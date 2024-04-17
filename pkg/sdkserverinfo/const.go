package sdkserverinfo

const (
	// Server information file configs
	MapServerInfoFile               = "/var/run/numaflow/mapper-server-info"
	MapStreamServerInfoFile         = "/var/run/numaflow/mapstreamer-server-info"
	ReduceServerInfoFile            = "/var/run/numaflow/reducer-server-info"
	ReduceStreamServerInfoFile      = "/var/run/numaflow/reducestreamer-server-info"
	SessionReduceServerInfoFile     = "/var/run/numaflow/sessionreducer-server-info"
	SideInputServerInfoFile         = "/var/run/numaflow/sideinput-server-info"
	SinkServerInfoFile              = "/var/run/numaflow/sinker-server-info"
	FbSinkServerInfoFile            = "/var/run/numaflow/fb-sinker-server-info"
	SourceServerInfoFile            = "/var/run/numaflow/sourcer-server-info"
	SourceTransformerServerInfoFile = "/var/run/numaflow/sourcetransformer-server-info"
)
