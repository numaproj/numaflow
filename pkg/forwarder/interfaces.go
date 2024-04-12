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

package forwarder

// VertexBuffer points to the partition of a buffer owned by the vertex.
type VertexBuffer struct {
	ToVertexName         string
	ToVertexPartitionIdx int32
}

// ToWhichStepDecider decides which step to forward after applying the WhereTo function.
type ToWhichStepDecider interface {
	// WhereTo decides where to forward the result based on the name of the step it returns.
	// It supports 2 additional keywords which need not be a step name. They are "ALL" and "DROP"
	// where former means, forward to all the neighbouring steps and latter means do not forward anywhere.
	//
	// Parameters:
	// - keys: Used by shuffle to decide which partition to write, if the toVertex is 'reduce' and has
	// multiple partitions. It is deterministic messages with same set of keys will always go to the same partition.
	//
	// - tags: Used for conditional forwarding.
	//
	// - id: Used by shuffle to decide which partition to write, if the toVertex is a 'map' and has
	// multiple partitions. It is deterministic messages with same id will always go to the same partition.
	WhereTo([]string, []string, string) ([]VertexBuffer, error)
}

// GoWhere is the step decider on where it needs to go
type GoWhere func([]string, []string, string) ([]VertexBuffer, error)

// WhereTo decides where the data goes to.
func (gw GoWhere) WhereTo(ks []string, ts []string, id string) ([]VertexBuffer, error) {
	return gw(ks, ts, id)
}

// StarterStopper starts/stops the forwarding.
type StarterStopper interface {
	Start() <-chan struct{}
	Stop()
	ForceStop()
}
