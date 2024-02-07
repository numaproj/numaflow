package sdkclient

const (
	UDS                       = "unix"
	WinStartTime              = "x-numaflow-win-start-time"
	WinEndTime                = "x-numaflow-win-end-time"
	DefaultGRPCMaxMessageSize = 64 * 1024 * 1024
	GlobalReduceAddr          = "/var/run/numaflow/globalreduce.sock"
)
