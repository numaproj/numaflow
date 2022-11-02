package shuffle

import (
	"hash"
	"hash/fnv"

	"github.com/numaproj/numaflow/pkg/isb"
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

// Shuffle functions returns a shuffled identifier.
func (s *Shuffle) Shuffle(key string) string {
	// hash of the message key returns a unique hashValue
	// mod of hashValue will decide which isb it will belong
	hashValue := s.generateHash(key)
	hashValue = hashValue % uint64(s.buffersCount)
	return s.bufferIdentifiers[hashValue]
}

// ShuffleMessages accepts list of isb messages and returns the mapping of isb to messages
func (s *Shuffle) ShuffleMessages(messages []*isb.Message) map[string][]*isb.Message {
	hashMap := make(map[string][]*isb.Message)
	for _, message := range messages {
		identifier := s.Shuffle(message.Key)
		hashMap[identifier] = append(hashMap[identifier], message)
	}
	return hashMap
}

func (s *Shuffle) generateHash(key string) uint64 {
	s.hash.Reset()
	_, _ = s.hash.Write([]byte(key))
	return s.hash.Sum64()
}
