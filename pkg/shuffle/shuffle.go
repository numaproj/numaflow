package shuffle

import (
	"context"
	"github.com/cespare/xxhash"
	"github.com/numaproj/numaflow/pkg/isb"
)

// Shuffle shuffles messages among ISB
type Shuffle struct {
	bufferIdentifiers []string
}

// NewShuffle accepts list of buffer identifiers(unique identifier of isb)
// and returns new shuffle instance
func NewShuffle(bufferIdentifiers []string) *Shuffle {
	return &Shuffle{
		bufferIdentifiers: bufferIdentifiers,
	}
}

// ShuffleMessages accepts list of isb messages and returns the mapping of isb to messages
func (s *Shuffle) ShuffleMessages(ctx context.Context, messages []*isb.Message) map[string][]*isb.Message {
	buffersCount := uint64(len(s.bufferIdentifiers))

	// hash of the message key returns a unique hashValue
	// mod of hashValue will decide which isb it will belong
	hashMap := make(map[string][]*isb.Message)
	for _, message := range messages {
		hashValue := s.hash(message.Key)
		hashValue = hashValue % buffersCount
		hashMap[s.bufferIdentifiers[hashValue]] = append(hashMap[s.bufferIdentifiers[hashValue]], message)
	}
	return hashMap
}

func (s *Shuffle) hash(key string) uint64 {
	return xxhash.Sum64String(key)
}
