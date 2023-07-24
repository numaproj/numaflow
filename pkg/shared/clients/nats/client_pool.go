package nats

import (
	"container/list"
	"context"
	"sync"
)

// ClientPool is a pool of NATS clients
type ClientPool struct {
	clients *list.List
	mutex   sync.Mutex
}

// NewClientPool returns a new pool of NATS clients of the given size
func NewClientPool(ctx context.Context, opts ...Option) (*ClientPool, error) {
	clients := list.New()
	options := defaultOptions()

	for _, o := range opts {
		o(options)
	}

	for i := 0; i < options.clientPoolSize; i++ {
		client, err := NewNATSClient(ctx)
		if err != nil {
			return nil, err
		}
		clients.PushBack(client)

	}
	return &ClientPool{clients: clients}, nil
}

// NextAvailableClient returns the next available NATS client
func (p *ClientPool) NextAvailableClient() *NATSClient {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.clients.Len() == 0 {
		return nil
	}

	// get the first client and move it to the back of the list
	front := p.clients.Front()
	p.clients.MoveToBack(front)
	return front.Value.(*NATSClient)
}

// CloseAll closes all the clients in the pool
func (p *ClientPool) CloseAll() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for e := p.clients.Front(); e != nil; e = e.Next() {
		e.Value.(*NATSClient).Close()
	}
}
