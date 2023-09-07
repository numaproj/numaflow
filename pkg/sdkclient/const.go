package sdkclient

const (
	UDS                   = "unix"
	TcpAddr               = ":55551"
	MapAddr               = "/var/run/numaflow/map.sock"
	ReduceAddr            = "/var/run/numaflow/reduce.sock"
	MapStreamAddr         = "/var/run/numaflow/mapstream.sock"
	SourceAddr            = "/var/run/numaflow/source.sock"
	SourceTransformerAddr = "/var/run/numaflow/sourcetransform.sock"
	ServerInfoFilePath    = "/var/run/numaflow/server-info"
	SideInputAddr         = "/var/run/numaflow/sideinput.sock"
	SinkAddr              = "/var/run/numaflow/sink.sock"
	WinStartTime          = "x-numaflow-win-start-time"
	WinEndTime            = "x-numaflow-win-end-time"
)
