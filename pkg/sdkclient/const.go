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
)
