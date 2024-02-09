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

package nats

import (
	"container/list"
	"context"
	"sync"
)

// ClientPool is a pool of NATS clients used on at the initial create connection phase.
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

// NextAvailableClient returns the next available NATS client. This code need not be optimized because this is
// not in hot code path. It is only during connection creation/startup.
func (p *ClientPool) NextAvailableClient() *Client {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.clients.Len() == 0 {
		return nil
	}

	// get the first client and move it to the back of the list
	front := p.clients.Front()
	p.clients.MoveToBack(front)
	return front.Value.(*Client)
}

// CloseAll closes all the clients in the pool
func (p *ClientPool) CloseAll() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for e := p.clients.Front(); e != nil; e = e.Next() {
		e.Value.(*Client).Close()
	}
}
