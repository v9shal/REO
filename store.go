package main

import (
	"sync"
	"time"
)

type Item struct {
	val       string
	expiresAt *time.Time
}
type MemoryStore struct {
	mu  sync.RWMutex
	mem map[string]Item
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		mu:  sync.RWMutex{},
		mem: make(map[string]Item),
	}
}
func (item Item) isExpired() bool {
	if item.expiresAt == nil {
		return false
	}
	return time.Now().After(*item.expiresAt)
}
func (s *MemoryStore) Set(key string, val string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mem[key] = Item{
		val:       val,
		expiresAt: nil,
	}
}
func (s *MemoryStore) Get(key string) (string, bool) {
	s.mu.RLock()
	val, ok := s.mem[key]
	if !ok {
		s.mu.RUnlock()
		return "", false
	}
	if !val.isExpired() {
		s.mu.RUnlock()
		return val.val, true
	}
	s.mu.RUnlock()
	s.Del(key)
	return "", false
}
func (s *MemoryStore) Del(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.mem[key]; exists {
		delete(s.mem, key)
		return 1
	}
	return 0
}
func (s *MemoryStore) Exist(key string) int {
	_, err := s.Get(key)
	if err == true {
		return 1
	}
	return 0

}

func (s *MemoryStore) Expire(key string, seconds int) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.mem[key]
	if !exists {
		return 0
	}

	if item.isExpired() {
		delete(s.mem, key)
		return 0
	}

	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	item.expiresAt = &deadline
	s.mem[key] = item
	return 1
}
func (s *MemoryStore) TTL(key string) int {
	s.mu.RLock()
	item, exists := s.mem[key]

	if !exists {
		s.mu.RUnlock()
		return -2
	}

	if item.isExpired() {
		s.mu.RUnlock()
		s.Del(key)
		return -2
	}

	if item.expiresAt == nil {
		s.mu.RUnlock()
		return -1
	}

	s.mu.RUnlock()
	remaining := int(time.Until(*item.expiresAt).Seconds())
	return remaining
}
