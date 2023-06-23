package scaling

import (
	"container/list"
	"sync"

	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
)

type entry struct {
	address string
	client  *daemonclient.DaemonClient
}

// ClientCache is a cache for daemon clients.
type ClientCache struct {
	clientsList *list.List
	clientsMap  map[string]*list.Element
	capacity    int
	sync.Mutex
}

func NewClientCache(capacity int) *ClientCache {
	return &ClientCache{
		clientsList: list.New(),
		clientsMap:  make(map[string]*list.Element),
		capacity:    capacity,
	}
}

// Put function puts a daemon client into the cache.
// If the cache is full, it will close the least recently used client and remove it from the cache.
func (c *ClientCache) Put(key string, client *daemonclient.DaemonClient) {
	c.Lock()
	defer c.Unlock()
	if c.clientsList.Len() == c.capacity {
		e := c.clientsList.Back().Value.(*entry)
		_ = e.client.Close()
		c.clientsList.Remove(c.clientsList.Back())
		delete(c.clientsMap, e.address)
	}
	c.clientsMap[key] = c.clientsList.PushFront(client)
}

// Get function gets a daemon client from the cache.
func (c *ClientCache) Get(key string) *daemonclient.DaemonClient {
	c.Lock()
	defer c.Unlock()
	if element, ok := c.clientsMap[key]; ok {
		c.clientsList.MoveToFront(element)
		return element.Value.(*entry).client
	}
	return nil
}

// DeleteAll function closes all the daemon clients in the cache and removes them from the cache.
func (c *ClientCache) DeleteAll() {
	c.Lock()
	defer c.Unlock()
	for _, element := range c.clientsMap {
		_ = element.Value.(*entry).client.Close()
	}
	c.clientsList.Init()
	c.clientsMap = make(map[string]*list.Element)
}
