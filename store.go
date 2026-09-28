package main

import (
	"sync"
	"time"
)

type Item struct {
	val       string
	expiresAt *time.Time
}

type Shard struct {
	mu  sync.RWMutex
	mem map[string]Item
}
type ShardedStore struct {
	shards []Shard
}

const NumShards = 32

func NewShardStore() *ShardedStore {
	shards := make([]Shard, NumShards)
	for i := range shards {
		shards[i].mem = make(map[string]Item)
	}
	return &ShardedStore{
		shards: shards,
	}
}

func fnvHash(key string) int {
	var hash uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return int(hash & (NumShards - 1))
}
func (item Item) isExpired() bool {
	if item.expiresAt == nil {
		return false
	}
	return time.Now().After(*item.expiresAt)
}
func (s *ShardedStore) Set(key string, val string) {
	index := fnvHash(key)
	shard := &s.shards[index]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.mem[key] = Item{
		val:       val,
		expiresAt: nil,
	}
}
func (s *ShardedStore) Get(key string) (string, bool) {
	index := fnvHash(key)
	shard := &s.shards[index]
	shard.mu.RLock()
	val, ok := shard.mem[key]
	if !ok {
		shard.mu.RUnlock()
		return "", false
	}
	if !val.isExpired() {
		shard.mu.RUnlock()
		return val.val, true
	}
	shard.mu.RUnlock()
	s.Del(key)
	return "", false
}
func (s *ShardedStore) Del(key string) int {
	index := fnvHash(key)
	shard := &s.shards[index]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, exists := shard.mem[key]; exists {
		delete(shard.mem, key)
		return 1
	}
	return 0
}
func (s *ShardedStore) Exist(key string) int {
	_, err := s.Get(key)
	if err == true {
		return 1
	}
	return 0

}

func (s *ShardedStore) Expire(key string, seconds int) int {
	index := fnvHash(key)
	shard := &s.shards[index]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	item, exists := shard.mem[key]
	if !exists {
		return 0
	}

	if item.isExpired() {
		delete(shard.mem, key)
		return 0
	}

	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	item.expiresAt = &deadline
	shard.mem[key] = item
	return 1
}
func (s *ShardedStore) TTL(key string) int {
	index := fnvHash(key)
	shard := &s.shards[index]
	shard.mu.RLock()
	item, exists := shard.mem[key]

	if !exists {
		shard.mu.RUnlock()
		return -2
	}

	if item.isExpired() {
		shard.mu.RUnlock()
		s.Del(key)
		return -2
	}

	if item.expiresAt == nil {
		shard.mu.RUnlock()
		return -1
	}

	shard.mu.RUnlock()
	remaining := int(time.Until(*item.expiresAt).Seconds())
	return remaining
}
