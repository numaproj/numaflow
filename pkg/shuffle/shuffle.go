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

package shuffle

import (
	"hash"
	"sync"

	"github.com/numaproj/numaflow/pkg/isb"

	"github.com/spaolacci/murmur3"
)

// Shuffle shuffles messages among ISB
type Shuffle struct {
	vertexName string
	// partitionCount is the number of partitions of the buffer owned by the vertex
	partitionCount int
	hash           hash.Hash64
	// we need to hold a lock because concurrent PnFs writes to buffer which internally invokes shuffle.
	// we need the lock to protect the hash.
	mu sync.Mutex
}

// NewShuffle accepts list of buffer identifiers(unique identifier of isb)
// and returns new shuffle instance. It uses vertex-name as seed, without a seed, we will end with the problem where
// Shuffling before the Vnth vertex creates a key to edge-buffer-index affinity,
// which will not change from Vn to Vn+1 vertices if there is no re-keying between these vertices causing
// idle partitions.
func NewShuffle(vertexName string, partitionCount int) *Shuffle {
	// We use vertex name as seed.
	vertexHash := murmur3.New64()
	_, _ = vertexHash.Write([]byte(vertexName))

	return &Shuffle{
		vertexName:     vertexName,
		partitionCount: partitionCount,
		// we use murmur3, we are open for suggestions. fnv did not work for us because of lack of re-keying in
		// some cases causing idle partitions in edges. We need to revisit the below link
		// https://softwareengineering.stackexchange.com/questions/49550/which-hashing-algorithm-is-best-for-uniqueness-and-speed
		hash: murmur3.New64WithSeed(uint32(vertexHash.Sum64())),
	}
}

// ShuffleOnKeys accepts array of keys and returns a shuffled identifier.
// We do not need message-id here because there is no multi-partitioning in Reduce streams.
func (s *Shuffle) ShuffleOnKeys(keys []string) int32 {
	// hash of the message keys returns a unique hashValue
	// mod of hashValue will decide which isb it will belong
	hashValue := s.generateHash(keys)
	hashValue = hashValue % uint64(s.partitionCount)
	return int32(hashValue)
}

// ShuffleOnId shuffle based on the message-id. This is used to make sure we
// always have consistent hashing for a given message-id. This is used for non-reduce vertex.
func (s *Shuffle) ShuffleOnId(msgId string) int32 {
	hashValue := s.generateHash([]string{msgId})
	hashValue = hashValue % uint64(s.partitionCount)
	return int32(hashValue)
}

// ShuffleMessages accepts list of isb messages and returns the mapping of isb to messages
func (s *Shuffle) ShuffleMessages(messages []*isb.Message) map[int32][]*isb.Message {
	hashMap := make(map[int32][]*isb.Message)
	for _, message := range messages {
		identifier := s.ShuffleOnKeys(message.Keys)
		hashMap[identifier] = append(hashMap[identifier], message)
	}
	return hashMap
}

func (s *Shuffle) generateHash(keys []string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hash.Reset()
	for _, k := range keys {
		_, _ = s.hash.Write([]byte(k))
	}
	return s.hash.Sum64()
}
