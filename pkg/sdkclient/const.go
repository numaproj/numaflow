package sdkclient

const (
	UDS                       = "unix"
	WinStartTime              = "x-numaflow-win-start-time"
	WinEndTime                = "x-numaflow-win-end-time"
	DefaultGRPCMaxMessageSize = 64 * 1024 * 1024

	// Socket configs
	MapAddr               = "/var/run/numaflow/map.sock"
	MapStreamAddr         = "/var/run/numaflow/mapstream.sock"
	ReduceAddr            = "/var/run/numaflow/reduce.sock"
	ReduceStreamAddr      = "/var/run/numaflow/reducestream.sock"
	SessionReduceAddr     = "/var/run/numaflow/sessionreduce.sock"
	SideInputAddr         = "/var/run/numaflow/sideinput.sock"
	SinkAddr              = "/var/run/numaflow/sink.sock"
	FbSinkAddr            = "/var/run/numaflow/fb-sink.sock"
	SourceAddr            = "/var/run/numaflow/source.sock"
	SourceTransformerAddr = "/var/run/numaflow/sourcetransform.sock"

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
