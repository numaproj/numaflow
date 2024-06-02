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
	MultiProcAddr         = "/var/run/numaflow/multiproc"
	FlatmapAddr           = "/var/run/numaflow/flatmap.sock"

	// Server information file configs
	FlatmapServerInfoFile           = "/var/run/numaflow/flatmapper-server-info"
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
