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

// Package wmb represents the offset-timeline pair and its corresponding encoder and decoder.
package wmb

import (
	"google.golang.org/protobuf/proto"

	wmbpb "github.com/numaproj/numaflow/pkg/apis/proto/wmb"
)

// WMB is used in the KV offset timeline bucket as the value for the given processor entity key.
type WMB struct {
	// Idle is set to true if the given processor entity hasn't published anything
	// to the offset timeline bucket in a batch processing cycle.
	// Idle is used to signal an idle watermark.
	Idle bool
	// Offset is the monotonically increasing index/offset of the buffer (buffer is the physical representation
	// of the partition of the edge).
	Offset int64
	// Watermark is tightly coupled with the offset and will be monotonically increasing for a given ProcessorEntity
	// as the offset increases.
	// When it is idling (Idle==true), for a given offset, the watermark can monotonically increase without offset
	// increasing.
	Watermark int64
	// Partition to identify the partition to which the watermark belongs.
	Partition int32
}

// EncodeToBytes encodes a WMB object into byte array using protobuf.
func (w WMB) EncodeToBytes() ([]byte, error) {
	pb := &wmbpb.WMB{
		Idle:      w.Idle,
		Offset:    w.Offset,
		Watermark: w.Watermark,
		Partition: w.Partition,
	}
	return proto.Marshal(pb)
}

// DecodeToWMB decodes the given byte array into a WMB object using protobuf.
func DecodeToWMB(b []byte) (WMB, error) {
	var pb wmbpb.WMB
	if err := proto.Unmarshal(b, &pb); err != nil {
		return WMB{}, err
	}

	return WMB{
		Idle:      pb.Idle,
		Offset:    pb.Offset,
		Watermark: pb.Watermark,
		Partition: pb.Partition,
	}, nil
}
