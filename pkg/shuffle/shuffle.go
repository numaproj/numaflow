package shuffle

import (
	"github.com/numaproj/numaflow/pkg/isb"
	"hash"
	"hash/fnv"
)

// Shuffle shuffles messages among ISB
type Shuffle struct {
	bufferIdentifiers []string
	buffersCount      int
	hash              hash.Hash64
}

// NewShuffle accepts list of buffer identifiers(unique identifier of isb)
// and returns new shuffle instance
func NewShuffle(bufferIdentifiers []string) *Shuffle {
	return &Shuffle{
		bufferIdentifiers: bufferIdentifiers,
		buffersCount:      len(bufferIdentifiers),
		hash:              fnv.New64(),
	}
}

// ShuffleMessages accepts list of isb messages and returns the mapping of isb to messages
func (s *Shuffle) ShuffleMessages(messages []*isb.Message) map[string][]*isb.Message {

	// hash of the message key returns a unique hashValue
	// mod of hashValue will decide which isb it will belong
	hashMap := make(map[string][]*isb.Message)
	for _, message := range messages {
		hashValue := s.generateHash(message.Key)
		hashValue = hashValue % uint64(s.buffersCount)
		hashMap[s.bufferIdentifiers[hashValue]] = append(hashMap[s.bufferIdentifiers[hashValue]], message)
	}
	return hashMap
}

func (s *Shuffle) generateHash(key string) uint64 {
	s.hash.Reset()
	_, _ = s.hash.Write([]byte(key))
	return s.hash.Sum64()
}
