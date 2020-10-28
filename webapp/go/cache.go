package main

import "sync"

type SCache struct {
	ma map[string]interface{}
	mu sync.RWMutex
}

func NewSC() *SCache {
	ma := make(map[string]interface{})
	return &SCache{ma: ma}
}

func (c *SCache) Get(k string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	r, ok := c.ma[k]
	return r, ok
}

func (c *SCache) Set(k string, v interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ma[k] = v
}

func (c *SCache) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ma = make(map[string]interface{})
}
