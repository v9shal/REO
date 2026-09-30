package main

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const maxVersion = 5

type Version struct {
	VersionId   uint64
	Timestamp   int64
	value       string
	isTombStone bool
	expiresAt   int64
}

type Shard struct {
	mu  sync.RWMutex
	mem map[string][]Version
}
type ShardedStore struct {
	shards          []Shard
	globalVersionID atomic.Uint64
}

const NumShards = 32

func NewShardStore() *ShardedStore {
	shards := make([]Shard, NumShards)
	for i := range shards {
		shards[i].mem = make(map[string][]Version)
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

func (v Version) isExpiredAt(targetTimeNano int64) bool {
	if v.expiresAt == 0 {
		return false
	}
	return targetTimeNano >= v.expiresAt
}
func addVersion(versions []Version, v Version) []Version {
	if len(versions) == maxVersion {
		copy(versions, versions[1:])
		versions[len(versions)-1] = v
		return versions
	}

	return append(versions, v)
}
func (s *ShardedStore) Set(key string, val string) uint64 {
	index := fnvHash(key)
	shard := &s.shards[index]
	vID := s.globalVersionID.Add(1)
	Version := Version{
		VersionId:   vID,
		Timestamp:   time.Now().UnixNano(),
		value:       val,
		expiresAt:   0,
		isTombStone: false,
	}
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.mem[key] = addVersion(shard.mem[key], Version)
	return vID

}
func (s *ShardedStore) Get(key string) (string, bool) {
	index := fnvHash(key)
	shard := &s.shards[index]
	shard.mu.RLock()
	versions, ok := shard.mem[key]

	if !ok {
		shard.mu.RUnlock()
		return "", false
	}
	if len(versions) == 0 {
		shard.mu.RUnlock()
		return "", false
	}
	latest_version := versions[len(versions)-1]
	if latest_version.isTombStone {
		shard.mu.RUnlock()
		return "", false
	}
	if latest_version.isExpiredAt(time.Now().UnixNano()) {
		shard.mu.RUnlock()
		return "", false
	}

	shard.mu.RUnlock()
	return latest_version.value, true
}
func (s *ShardedStore) Del(key string) int {
	index := fnvHash(key)
	shard := &s.shards[index]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	version, exists := shard.mem[key]
	if !exists || len(version) == 0 {
		return 0
	}

	latest_version := version[len(version)-1]
	now := time.Now().UnixNano()

	if latest_version.isTombStone || latest_version.isExpiredAt(now) {
		return 0
	}

	vID := s.globalVersionID.Add(1)
	tombstone := Version{
		VersionId:   vID,
		Timestamp:   now,
		value:       "",
		isTombStone: true,
		expiresAt:   0,
	}

	shard.mem[key] = addVersion(shard.mem[key], tombstone)
	return 1
}
func (s *ShardedStore) History(key string) []Version {
	index := fnvHash(key)
	shard := &s.shards[index]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	version := shard.mem[key]
	return version
}
func (s *ShardedStore) AsOf(key string, targetTimeNano int64) (string, bool) {
	index := fnvHash(key)
	shard := &s.shards[index]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	versions, ok := shard.mem[key]
	if !ok || len(versions) == 0 {
		return "", false
	}
	if targetTimeNano < versions[0].Timestamp {
		return "", false
	}
	idx := sort.Search(len(versions), func(i int) bool {
		return versions[i].Timestamp > targetTimeNano
	})
	if idx == len(versions) {
		idx = len(versions) - 1
	} else {
		idx--
	}
	version := versions[idx]
	if version.isTombStone {
		return "", false
	}
	if version.isExpiredAt(targetTimeNano) {
		return "", false
	}
	return version.value, true

}

func (s *ShardedStore) Rollback(key string, targetVersionId uint64) (string, bool) {
	index := fnvHash(key)
	shard := &s.shards[index]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	version := shard.mem[key]
	for _, v := range version {
		if v.VersionId == targetVersionId {
			if v.isTombStone {
				return "", false
			}
			value := v.value
			vID := s.globalVersionID.Add(1)
			ver := Version{
				VersionId:   vID,
				Timestamp:   time.Now().UnixNano(),
				isTombStone: false,
				value:       value,
				expiresAt:   0,
			}
			shard.mem[key] = addVersion(shard.mem[key], ver)
			return value, true
		}
	}
	return "", false

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
	version, exists := shard.mem[key]
	if !exists || len(version) == 0 {
		return 0
	}
	v := version[len(version)-1]
	if v.isTombStone || v.isExpiredAt(time.Now().UnixNano()) {
		return 0
	}
	version[len(version)-1].expiresAt = time.Now().Add(time.Duration(seconds) * time.Second).UnixNano()
	return 1
}
func (s *ShardedStore) TTL(key string) int {
	index := fnvHash(key)
	shard := &s.shards[index]
	shard.mu.RLock()
	version, exists := shard.mem[key]
	if !exists || len(version) == 0 {
		shard.mu.RUnlock()
		return -2
	}
	v := version[len(version)-1]
	if !exists {
		shard.mu.RUnlock()
		return -2
	}

	if v.isExpiredAt(time.Now().UnixNano()) {
		shard.mu.RUnlock()
		return -2
	}

	if v.expiresAt == 0 {
		shard.mu.RUnlock()
		return -1
	}

	shard.mu.RUnlock()
	now := time.Now().UnixNano()
	if v.expiresAt <= now {
		return -2
	}
	remaining := int((v.expiresAt - now) / int64(time.Second))
	return remaining
}
