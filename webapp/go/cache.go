package main

import (
	"sync"
)

type SCache struct {
	ma map[string]interface{}
	mu sync.RWMutex
}

type EsCache struct {
	ma map[int64]Estate
	mu sync.RWMutex
}

type ChCache struct {
	ma map[int64]Chair
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

func NewCC() *ChCache {
	ma := make(map[int64]Chair)
	return &ChCache{ma: ma}
}

func (c *ChCache) Get(k int64) (Chair, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	r, ok := c.ma[k]
	return r, ok
}

func (c *ChCache) GetMulti(ks []int64) ([]Chair, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res := []Chair{}
	for _, k := range ks {
		if r, ok := c.ma[k]; ok {
			res = append(res, r)
		}
	}
	return res, len(ks) == len(res)
}

func (c *ChCache) Set(k int64, v Chair) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ma[k] = v
}

func (c *ChCache) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ma = make(map[int64]Chair)
}

func NewIC() *EsCache {
	ma := make(map[int64]Estate)
	return &EsCache{ma: ma}
}

func (c *EsCache) Get(k int64) (Estate, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	r, ok := c.ma[k]
	return r, ok
}

func (c *EsCache) GetMulti(ks []int64) ([]Estate, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res := []Estate{}
	for _, k := range ks {
		if r, ok := c.ma[k]; ok {
			res = append(res, r)
		}
	}
	return res, len(ks) == len(res)
}

func (c *EsCache) Set(k int64, v Estate) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ma[k] = v
}

func (c *EsCache) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ma = make(map[int64]Estate)
}
