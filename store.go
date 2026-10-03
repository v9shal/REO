package main

import (
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxVersion = 5
	NumShards  = 32
)

// maxKeysInRam is the soft cap on keys held in RAM; colder keys spill to disk.
// Override with the REO_MAX_RAM_KEYS environment variable (default 5, which
// forces almost everything through the disk tier).
var maxKeysInRam = func() int64 {
	if v, err := strconv.ParseInt(os.Getenv("REO_MAX_RAM_KEYS"), 10, 64); err == nil && v > 0 {
		return v
	}
	return 5
}()

type StorageLocation uint8

const (
	LocationRam StorageLocation = iota
	LocationDisk
)

type KeyEntry struct {
	Location     StorageLocation
	Versions     []Version
	Stub         DiskStub
	LastAccessed int64
}

type Version struct {
	VersionId   uint64 `json:"version_id"`
	Timestamp   int64  `json:"timestamp"`
	Value       string `json:"value"`
	IsTombStone bool   `json:"is_tombstone"`
	ExpiresAt   int64  `json:"expires_at"`
}

type Shard struct {
	mu  sync.RWMutex
	mem map[string]*KeyEntry
	// ram is the set of keys in mem whose Location is LocationRam. It stays
	// small (bounded by the RAM cap), so picking an eviction victim never has
	// to walk the (huge) population of disk-resident keys.
	ram map[string]struct{}
}

type ShardedStore struct {
	shards          []Shard
	globalVersionID atomic.Uint64
	disk            *DiskEngine
	ramKeys         atomic.Int64
}

// --------------------------------------------------
// Constructor
// --------------------------------------------------

func NewShardStore(filePath string) (*ShardedStore, error) {
	disk, err := NewDiskEngine(filePath)
	if err != nil {
		return nil, err
	}

	shards := make([]Shard, NumShards)

	for i := range shards {
		shards[i].mem = make(map[string]*KeyEntry)
		shards[i].ram = make(map[string]struct{})
	}

	return &ShardedStore{
		shards: shards,
		disk:   disk,
	}, nil
}

// --------------------------------------------------
// Hashing
// --------------------------------------------------

func fnvHash(key string) int {
	var hash uint32 = 2166136261

	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}

	return int(hash & (NumShards - 1))
}

// --------------------------------------------------
// Version helpers
// --------------------------------------------------

func (v Version) isExpiredAt(targetTimeNano int64) bool {
	if v.ExpiresAt == 0 {
		return false
	}

	return targetTimeNano >= v.ExpiresAt
}

func addVersion(versions []Version, v Version) []Version {
	if len(versions) == maxVersion {
		copy(versions, versions[1:])

		versions[len(versions)-1] = v

		return versions
	}

	return append(versions, v)
}

// --------------------------------------------------
// Disk loading / hydration
// --------------------------------------------------

// hydrate makes sure the key's versions are available in RAM.
//
// If the key is already in RAM:
//
//	return the existing versions.
//
// If the key is on disk:
//
//	read the disk data
//	deserialize it
//	put it back into RAM
//	change LocationDisk -> LocationRam
func (s *ShardedStore) hydrate(
	shard *Shard,
	key string,
) ([]Version, bool, error) {

	entry, exists := shard.mem[key]

	if !exists {
		return nil, false, nil
	}
	entry.LastAccessed = time.Now().UnixNano()

	// Already in RAM.
	if entry.Location == LocationRam {
		return entry.Versions, true, nil
	}

	// Load from disk.
	data, err := s.disk.Read(entry.Stub)
	if err != nil {
		return nil, false, err
	}

	versions, err := deserializeVersions(data)
	if err != nil {
		return nil, false, err
	}

	// Put data back into RAM.
	entry.Versions = versions
	entry.Location = LocationRam
	shard.ram[key] = struct{}{}
	s.ramKeys.Add(1)

	return versions, true, nil
}

// loadFromDisk is a small helper used when we already
// have a KeyEntry.
func (s *ShardedStore) loadFromDisk(
	entry *KeyEntry,
) ([]Version, error) {

	data, err := s.disk.Read(entry.Stub)
	if err != nil {
		return nil, err
	}

	return deserializeVersions(data)
}

// --------------------------------------------------
// SET
// --------------------------------------------------

func (s *ShardedStore) Set(key string, val string) uint64 {
	// The locked section lives in its own closure so `defer` guarantees the
	// shard lock is released on EVERY return path (including errors).
	vID := func() uint64 {
		shard := &s.shards[fnvHash(key)]
		shard.mu.Lock()
		defer shard.mu.Unlock()

		entry, exists := shard.mem[key]
		if !exists {
			entry = &KeyEntry{
				Location: LocationRam,
				Versions: make([]Version, 0, maxVersion),
			}
			shard.mem[key] = entry
			shard.ram[key] = struct{}{}
			s.ramKeys.Add(1)
		}
		entry.LastAccessed = time.Now().UnixNano()

		// If key is on disk, hydrate it first.
		if entry.Location == LocationDisk {
			versions, err := s.loadFromDisk(entry)
			if err != nil {
				return 0
			}
			entry.Versions = versions
			entry.Location = LocationRam
			shard.ram[key] = struct{}{}
			s.ramKeys.Add(1)
		}

		id := s.globalVersionID.Add(1)
		entry.Versions = addVersion(entry.Versions, Version{
			VersionId: id,
			Timestamp: time.Now().UnixNano(),
			Value:     val,
		})
		return id
	}()

	// Evict AFTER releasing the lock (evictOneColdKey takes another shard's lock).
	if vID != 0 && s.ramKeys.Load() > maxKeysInRam {
		s.evictOneColdKey()
	}
	return vID
}

// --------------------------------------------------
// GET
// --------------------------------------------------

func (s *ShardedStore) Get(key string) (string, bool) {
	index := fnvHash(key)
	shard := &s.shards[index]

	// We need Lock instead of RLock because hydrate()
	// can modify the KeyEntry.
	shard.mu.Lock()
	defer shard.mu.Unlock()

	versions, ok, err := s.hydrate(shard, key)

	if err != nil || !ok || len(versions) == 0 {
		return "", false
	}

	latest := versions[len(versions)-1]

	now := time.Now().UnixNano()

	// Deleted.
	if latest.IsTombStone {
		return "", false
	}

	// Expired.
	if latest.isExpiredAt(now) {
		return "", false
	}

	return latest.Value, true
}

// --------------------------------------------------
// DELETE
// --------------------------------------------------

func (s *ShardedStore) Del(key string) int {
	index := fnvHash(key)
	shard := &s.shards[index]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	versions, exists, err := s.hydrate(shard, key)

	if err != nil || !exists || len(versions) == 0 {
		return 0
	}

	latest := versions[len(versions)-1]

	now := time.Now().UnixNano()

	// Already deleted.
	if latest.IsTombStone {
		return 0
	}

	// Already expired.
	if latest.isExpiredAt(now) {
		return 0
	}

	// Create tombstone.
	tombstone := Version{
		VersionId:   s.globalVersionID.Add(1),
		Timestamp:   now,
		Value:       "",
		IsTombStone: true,
		ExpiresAt:   0,
	}

	entry := shard.mem[key]

	entry.Versions = addVersion(
		entry.Versions,
		tombstone,
	)

	return 1
}

// --------------------------------------------------
// HISTORY
// --------------------------------------------------

func (s *ShardedStore) History(key string) []Version {
	index := fnvHash(key)
	shard := &s.shards[index]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	versions, ok, err := s.hydrate(shard, key)

	if err != nil || !ok {
		return nil
	}

	// Return a copy so callers cannot modify internal state.
	return append([]Version(nil), versions...)
}

// --------------------------------------------------
// AS OF
// --------------------------------------------------

func (s *ShardedStore) AsOf(
	key string,
	targetTimeNano int64,
) (string, bool) {

	index := fnvHash(key)
	shard := &s.shards[index]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	versions, ok, err := s.hydrate(shard, key)

	if err != nil || !ok || len(versions) == 0 {
		return "", false
	}

	// Target is older than the oldest available version.
	if targetTimeNano < versions[0].Timestamp {
		return "", false
	}

	// Find first version whose timestamp > target.
	idx := sort.Search(len(versions), func(i int) bool {
		return versions[i].Timestamp > targetTimeNano
	})

	// This shouldn't normally happen because of the
	// first boundary check, but keep it safe.
	if idx == 0 {
		return "", false
	}

	// We need the previous version.
	idx--

	version := versions[idx]

	// Deleted at that point in time.
	if version.IsTombStone {
		return "", false
	}

	// Expired at that point in time.
	if version.isExpiredAt(targetTimeNano) {
		return "", false
	}

	return version.Value, true
}

// --------------------------------------------------
// ROLLBACK
// --------------------------------------------------

func (s *ShardedStore) Rollback(
	key string,
	targetVersionId uint64,
) (string, bool) {

	index := fnvHash(key)
	shard := &s.shards[index]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	versions, ok, err := s.hydrate(shard, key)

	if err != nil || !ok || len(versions) == 0 {
		return "", false
	}

	var target *Version

	for i := range versions {
		if versions[i].VersionId == targetVersionId {
			target = &versions[i]
			break
		}
	}

	if target == nil {
		return "", false
	}

	// Cannot rollback to a tombstone.
	if target.IsTombStone {
		return "", false
	}

	// Create a NEW version.
	//
	// We don't modify the old version because
	// version history should remain immutable.
	newVersion := Version{
		VersionId:   s.globalVersionID.Add(1),
		Timestamp:   time.Now().UnixNano(),
		Value:       target.Value,
		IsTombStone: false,
		ExpiresAt:   0,
	}

	entry := shard.mem[key]

	entry.Versions = addVersion(
		entry.Versions,
		newVersion,
	)

	return newVersion.Value, true
}

// --------------------------------------------------
// EXISTS
// --------------------------------------------------

func (s *ShardedStore) Exist(key string) int {
	if _, ok := s.Get(key); ok {
		return 1
	}

	return 0
}

// --------------------------------------------------
// EXPIRE
// --------------------------------------------------

func (s *ShardedStore) Expire(
	key string,
	seconds int,
) int {

	index := fnvHash(key)
	shard := &s.shards[index]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	versions, exists, err := s.hydrate(shard, key)

	if err != nil || !exists || len(versions) == 0 {
		return 0
	}

	latest := &versions[len(versions)-1]

	now := time.Now().UnixNano()

	// Key is deleted.
	if latest.IsTombStone {
		return 0
	}

	// Already expired.
	if latest.isExpiredAt(now) {
		return 0
	}

	latest.ExpiresAt = time.Now().
		Add(time.Duration(seconds) * time.Second).
		UnixNano()

	return 1
}

// --------------------------------------------------
// TTL
// --------------------------------------------------

func (s *ShardedStore) TTL(key string) int {
	index := fnvHash(key)
	shard := &s.shards[index]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	versions, exists, err := s.hydrate(shard, key)

	if err != nil || !exists || len(versions) == 0 {
		return -2
	}

	latest := versions[len(versions)-1]

	now := time.Now().UnixNano()

	// Deleted.
	if latest.IsTombStone {
		return -2
	}

	// No expiration.
	if latest.ExpiresAt == 0 {
		return -1
	}

	// Expired.
	if latest.ExpiresAt <= now {
		return -2
	}

	remaining := latest.ExpiresAt - now

	return int(remaining / int64(time.Second))
}

// --------------------------------------------------
// EVICT
// --------------------------------------------------

// Evict moves a key's versions from RAM to disk.
//
// Before:
//
//	LocationRam
//	Versions = [v1,v2,v3,v4,v5]
//
// After:
//
//	LocationDisk
//	Versions = nil
//	Stub = {offset,length}
func (s *ShardedStore) Evict(key string) error {
	index := fnvHash(key)
	shard := &s.shards[index]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	return s.evict(shard, key)
}

// Internal eviction function.
// Caller must hold shard.mu.
func (s *ShardedStore) evict(
	shard *Shard,
	key string,
) error {

	entry, exists := shard.mem[key]

	if !exists {
		return nil
	}

	// Already on disk.
	if entry.Location == LocationDisk {
		return nil
	}

	// Nothing to write.
	if len(entry.Versions) == 0 {
		return nil
	}

	// Serialize versions.
	data, err := serializeVersions(entry.Versions)

	if err != nil {
		return err
	}

	// Write to disk.
	stub, err := s.disk.Write(data)

	if err != nil {
		return err
	}
	s.ramKeys.Add(-1)
	// Free versions from RAM.
	entry.Versions = nil

	// Save disk location.
	entry.Stub = stub
	entry.Location = LocationDisk
	delete(shard.ram, key)

	return nil
}

// evictOneColdKey moves one cold RAM-resident key to disk.
//
// It starts at a random shard and walks forward until a shard that actually
// has RAM-resident keys is found (picking a single random shard often hit an
// empty one, which let the RAM cap leak). Inside the shard it samples a few
// keys from the small RAM set and evicts the least recently used of them, so
// the cost per call is O(1) no matter how many keys live on disk.
func (s *ShardedStore) evictOneColdKey() {
	start := rand.Intn(NumShards)
	for i := 0; i < NumShards; i++ {
		if s.evictColdFromShard(&s.shards[(start+i)%NumShards]) {
			return
		}
	}
}

func (s *ShardedStore) evictColdFromShard(shard *Shard) bool {
	const sampleSize = 8

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if len(shard.ram) == 0 {
		return false
	}

	// Go randomizes map iteration order, so the first few entries are a
	// random sample.
	var victim string
	oldest := int64(math.MaxInt64)
	seen := 0
	for key := range shard.ram {
		if e := shard.mem[key]; e != nil && e.LastAccessed < oldest {
			oldest, victim = e.LastAccessed, key
		}
		if seen++; seen >= sampleSize {
			break
		}
	}
	if victim == "" {
		return false
	}
	return s.evict(shard, victim) == nil
}
