package main

import (
	"sync"

	"github.com/isucon/isucon10-qualify/isuumo/types"
)

type SCache struct {
	ma map[string]interface{}
	mu sync.RWMutex
}

type EsCache struct {
	ma map[int64]types.Estate
	mu sync.RWMutex
}

type ChCache struct {
	ma map[int64]types.Chair
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

func (c *SCache) FlushWithNew(k string, v interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := make(map[string]interface{})
	n[k] = v
	c.ma = n
}

func NewCC() *ChCache {
	ma := make(map[int64]types.Chair)
	return &ChCache{ma: ma}
}

func (c *ChCache) Get(k int64) (types.Chair, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	r, ok := c.ma[k]
	return r, ok
}

func (c *ChCache) GetMulti(ks []int64, arr *[]types.Chair) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res := *arr
	res = res[:0]
	for _, k := range ks {
		if r, ok := c.ma[k]; ok {
			res = append(res, r)
		}
	}
	*arr = res
}

func (c *ChCache) Set(k int64, v types.Chair) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ma[k] = v
}

func (c *ChCache) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ma = make(map[int64]types.Chair)
}

func NewIC() *EsCache {
	ma := make(map[int64]types.Estate)
	return &EsCache{ma: ma}
}

func (c *EsCache) Get(k int64) (types.Estate, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	r, ok := c.ma[k]
	return r, ok
}

func (c *EsCache) GetMulti(ks []int64, arr *[]types.Estate) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res := *arr
	res = res[:0]
	for _, k := range ks {
		if r, ok := c.ma[k]; ok {
			res = append(res, r)
		}
	}
	*arr = res
}

func (c *EsCache) Set(k int64, v types.Estate) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ma[k] = v
}

func (c *EsCache) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ma = make(map[int64]types.Estate)
}
