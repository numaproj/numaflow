/*
Copyright 2026 The Numaproj Authors.

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

package v1

// ISBJetStreamDTO is the response payload for JetStream monitor data on an ISB service.
type ISBJetStreamDTO struct {
	Summary       []JetStreamSummaryDTO  `json:"summary"`
	RaftMetaGroup []JetStreamRaftMetaDTO `json:"raftMetaGroup"`
	Errors        []ISBMonitorErrorDTO   `json:"errors,omitempty"`
}

// JetStreamSummaryDTO contains per-server JetStream monitor summary metrics.
type JetStreamSummaryDTO struct {
	Server       string  `json:"server"`
	ServerID     string  `json:"serverId,omitempty"`
	Cluster      string  `json:"cluster,omitempty"`
	Streams      int     `json:"streams"`
	Consumers    int     `json:"consumers"`
	Messages     uint64  `json:"messages"`
	Bytes        uint64  `json:"bytes"`
	APIRequests  uint64  `json:"apiRequests"`
	APIErrors    uint64  `json:"apiErrors"`
	APIErrorRate float64 `json:"apiErrorRate"`
	MetaLeader   bool    `json:"metaLeader"`
}

// JetStreamRaftMetaDTO contains RAFT meta group peer status for a JetStream cluster.
type JetStreamRaftMetaDTO struct {
	Name    string  `json:"name"`
	ID      string  `json:"id,omitempty"`
	Leader  bool    `json:"leader"`
	Current *bool   `json:"current,omitempty"`
	Online  bool    `json:"online"`
	Active  string  `json:"active,omitempty"`
	Lag     *uint64 `json:"lag,omitempty"`
}

// ISBMonitorErrorDTO captures a per-pod JetStream monitor collection error.
type ISBMonitorErrorDTO struct {
	Pod     string `json:"pod"`
	Message string `json:"message"`
}
