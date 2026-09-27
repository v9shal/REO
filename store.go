package main

import "sync"

type MemoryStore struct {
	mu  sync.RWMutex
	mem map[string]string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		mu:  sync.RWMutex{},
		mem: make(map[string]string),
	}
}
func (s *MemoryStore) Set(key string, val string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mem[key] = val
}
func (s *MemoryStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.mem[key]
	return val, ok
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
