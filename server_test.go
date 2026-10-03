package main

// Integration + invariant tests for REO.
//
//	go test ./...            # everything
//	go test -race ./...      # strongly recommended (needs cgo/gcc)
//	go test -run TestRamKeys -v
//
// Most tests talk to a real TCP listener running the real handleConnection,
// so they exercise the parser, handlers, store and disk tier end to end.
// Three tests are white-box regression tests for bugs in the tiered storage /
// parser; they are marked "REGRESSION" below.

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------- helpers

func encodeTestCmd(args ...string) []byte {
	var b strings.Builder
	b.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, a := range args {
		b.WriteString("$" + strconv.Itoa(len(a)) + "\r\n" + a + "\r\n")
	}
	return []byte(b.String())
}

type reply struct {
	kind byte     // '+', '-', ':', '$', '*'
	str  string   // simple string / error text / integer text / bulk payload
	null bool     // null bulk string
	arr  []string // payloads of an array reply
}

func (r reply) num() int {
	n, _ := strconv.Atoi(r.str)
	return n
}

func readTestReply(r *bufio.Reader) (reply, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return reply{}, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line == "" {
		return reply{}, fmt.Errorf("empty reply line")
	}
	kind, body := line[0], line[1:]
	switch kind {
	case '+', '-', ':':
		return reply{kind: kind, str: body}, nil
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil {
			return reply{}, err
		}
		if n < 0 {
			return reply{kind: kind, null: true}, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return reply{}, err
		}
		return reply{kind: kind, str: string(buf[:n])}, nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil {
			return reply{}, err
		}
		rep := reply{kind: kind}
		for i := 0; i < n; i++ {
			sub, err := readTestReply(r)
			if err != nil {
				return reply{}, err
			}
			rep.arr = append(rep.arr, sub.str)
		}
		return rep, nil
	}
	return reply{}, fmt.Errorf("unknown reply type in %q", line)
}

type tcClient struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func tcDial(addr string) (*tcClient, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	return &tcClient{conn: conn, r: bufio.NewReader(conn)}, nil
}

// try is safe to call from non-test goroutines (it never calls t.Fatal).
func (c *tcClient) try(args ...string) (reply, error) {
	c.conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.conn.Write(encodeTestCmd(args...)); err != nil {
		return reply{}, err
	}
	return readTestReply(c.r)
}

func (c *tcClient) do(args ...string) reply {
	c.t.Helper()
	rep, err := c.try(args...)
	if err != nil {
		c.t.Fatalf("%v: %v", args, err)
	}
	return rep
}

func startServer(t *testing.T) (string, *ShardedStore) {
	t.Helper()
	store, err := NewShardStore(filepath.Join(t.TempDir(), "data.log"))
	if err != nil {
		t.Fatalf("NewShardStore: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleConnection(conn, store)
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		store.disk.file.Close()
	})
	return ln.Addr().String(), store
}

func newClient(t *testing.T) (*tcClient, *ShardedStore) {
	t.Helper()
	addr, store := startServer(t)
	c, err := tcDial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c.t = t
	t.Cleanup(func() { c.conn.Close() })
	return c, store
}

func expect(t *testing.T, got reply, kind byte, str string) {
	t.Helper()
	if got.kind != kind || got.str != str {
		t.Fatalf("got %c %q (null=%v), want %c %q", got.kind, got.str, got.null, kind, str)
	}
}

func expectNull(t *testing.T, got reply) {
	t.Helper()
	if got.kind != '$' || !got.null {
		t.Fatalf("got %c %q, want null bulk string", got.kind, got.str)
	}
}

// parseHistory splits "v<id> | <ts> | <value>" entries.
func parseHistory(t *testing.T, arr []string) (ids []uint64, values []string) {
	t.Helper()
	for _, e := range arr {
		parts := strings.SplitN(e, " | ", 3)
		if len(parts) != 3 || !strings.HasPrefix(parts[0], "v") {
			t.Fatalf("bad HISTORY entry %q", e)
		}
		id, err := strconv.ParseUint(parts[0][1:], 10, 64)
		if err != nil {
			t.Fatalf("bad version id in %q", e)
		}
		ids = append(ids, id)
		values = append(values, parts[2])
	}
	return
}

// ---------------------------------------------------------------- basic commands

func TestPing(t *testing.T) {
	c, _ := newClient(t)
	expect(t, c.do("PING"), '+', "PONG")
	expect(t, c.do("PING", "hello"), '$', "hello")
	expect(t, c.do("ping"), '+', "PONG") // commands are case-insensitive
}

func TestSetGetOverwrite(t *testing.T) {
	c, _ := newClient(t)
	expectNull(t, c.do("GET", "missing"))
	expect(t, c.do("SET", "k", "v1"), '+', "OK")
	expect(t, c.do("GET", "k"), '$', "v1")
	expect(t, c.do("set", "k", "v2"), '+', "OK")
	expect(t, c.do("get", "k"), '$', "v2")
}

func TestValuesWithSpacesCRLFAndEmpty(t *testing.T) {
	c, _ := newClient(t)
	for _, v := range []string{"hello world", "line1\r\nline2", "", "$5\r\n*3\r\n", strings.Repeat("é世界", 50)} {
		expect(t, c.do("SET", "k", v), '+', "OK")
		expect(t, c.do("GET", "k"), '$', v)
	}
}

func TestLargeValueSurvivesEviction(t *testing.T) {
	c, _ := newClient(t)
	big := strings.Repeat("0123456789abcdef", 64*1024) // 1 MiB
	expect(t, c.do("SET", "big", big), '+', "OK")
	expect(t, c.do("EVICT", "big"), '+', "OK")
	expect(t, c.do("GET", "big"), '$', big)
}

func TestDelAndExists(t *testing.T) {
	c, _ := newClient(t)
	expect(t, c.do("EXISTS", "k"), ':', "0")
	expect(t, c.do("DEL", "k"), ':', "0")
	c.do("SET", "k", "v")
	expect(t, c.do("EXISTS", "k"), ':', "1")
	expect(t, c.do("DEL", "k"), ':', "1")
	expectNull(t, c.do("GET", "k"))
	expect(t, c.do("EXISTS", "k"), ':', "0")
	expect(t, c.do("DEL", "k"), ':', "0") // already deleted
	expect(t, c.do("SET", "k", "again"), '+', "OK")
	expect(t, c.do("GET", "k"), '$', "again")
}

func TestExpireAndTTL(t *testing.T) {
	c, _ := newClient(t)
	expect(t, c.do("TTL", "nope"), ':', "-2")
	expect(t, c.do("EXPIRE", "nope", "10"), ':', "0")

	c.do("SET", "k", "v")
	expect(t, c.do("TTL", "k"), ':', "-1") // no expiry
	expect(t, c.do("EXPIRE", "k", "100"), ':', "1")
	if ttl := c.do("TTL", "k").num(); ttl < 98 || ttl > 100 {
		t.Fatalf("TTL = %d, want ~100", ttl)
	}
	expect(t, c.do("GET", "k"), '$', "v")

	// expiry must survive a round trip through the disk tier
	expect(t, c.do("EVICT", "k"), '+', "OK")
	if ttl := c.do("TTL", "k").num(); ttl < 98 || ttl > 100 {
		t.Fatalf("TTL after evict = %d, want ~100", ttl)
	}
}

func TestKeyActuallyExpires(t *testing.T) {
	if testing.Short() {
		t.Skip("sleeps ~1.2s")
	}
	c, _ := newClient(t)
	c.do("SET", "k", "v")
	expect(t, c.do("EXPIRE", "k", "1"), ':', "1")
	time.Sleep(1200 * time.Millisecond)
	expectNull(t, c.do("GET", "k"))
	expect(t, c.do("EXISTS", "k"), ':', "0")
	expect(t, c.do("TTL", "k"), ':', "-2")
}

// ---------------------------------------------------------------- versioning

func TestHistoryCapsAtFiveVersions(t *testing.T) {
	c, _ := newClient(t)
	for i := 1; i <= 8; i++ {
		c.do("SET", "k", fmt.Sprintf("v%d", i))
	}
	rep := c.do("HISTORY", "k")
	ids, vals := parseHistory(t, rep.arr)
	if len(vals) != 5 {
		t.Fatalf("history length = %d, want 5 (maxVersion)", len(vals))
	}
	want := []string{"v4", "v5", "v6", "v7", "v8"}
	for i := range want {
		if vals[i] != want[i] {
			t.Fatalf("history values = %v, want %v", vals, want)
		}
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("version ids not strictly increasing: %v", ids)
		}
	}
}

func TestHistoryEmptyForMissingKey(t *testing.T) {
	c, _ := newClient(t)
	rep := c.do("HISTORY", "nope")
	if rep.kind != '*' || len(rep.arr) != 0 {
		t.Fatalf("got %c %v, want empty array", rep.kind, rep.arr)
	}
}

func TestHistoryShowsTombstoneAndSurvivesEviction(t *testing.T) {
	c, _ := newClient(t)
	c.do("SET", "k", "a")
	c.do("SET", "k", "b")
	c.do("DEL", "k")
	expect(t, c.do("EVICT", "k"), '+', "OK")

	_, vals := parseHistory(t, c.do("HISTORY", "k").arr)
	if len(vals) != 3 || vals[0] != "a" || vals[1] != "b" || vals[2] != "[DELETED]" {
		t.Fatalf("history after evict = %v", vals)
	}
	expectNull(t, c.do("GET", "k"))
}

func TestAsOf(t *testing.T) {
	c, _ := newClient(t)
	before := time.Now().UnixNano()
	time.Sleep(10 * time.Millisecond)
	c.do("SET", "k", "one")
	time.Sleep(10 * time.Millisecond)
	mid := time.Now().UnixNano()
	time.Sleep(10 * time.Millisecond)
	c.do("SET", "k", "two")
	after := time.Now().UnixNano()

	expectNull(t, c.do("AS.OF", "k", strconv.FormatInt(before, 10)))
	expect(t, c.do("AS.OF", "k", strconv.FormatInt(mid, 10)), '$', "one")
	expect(t, c.do("ASOF", "k", strconv.FormatInt(after, 10)), '$', "two")

	// still correct after the key has been moved to disk
	c.do("EVICT", "k")
	expect(t, c.do("AS.OF", "k", strconv.FormatInt(mid, 10)), '$', "one")

	// tombstone at that point in time
	c.do("DEL", "k")
	expectNull(t, c.do("AS.OF", "k", strconv.FormatInt(time.Now().UnixNano(), 10)))
	expect(t, c.do("AS.OF", "k", strconv.FormatInt(mid, 10)), '$', "one")
}

func TestRollback(t *testing.T) {
	c, _ := newClient(t)
	c.do("SET", "k", "a")
	c.do("SET", "k", "b")
	c.do("SET", "k", "c")
	ids, _ := parseHistory(t, c.do("HISTORY", "k").arr)

	expect(t, c.do("ROLLBACK", "k", strconv.FormatUint(ids[0], 10)), '+', "OK")
	expect(t, c.do("GET", "k"), '$', "a")

	// history is append-only: rollback created a NEW version
	_, vals := parseHistory(t, c.do("HISTORY", "k").arr)
	if len(vals) != 4 || vals[3] != "a" {
		t.Fatalf("history after rollback = %v", vals)
	}

	if r := c.do("ROLLBACK", "k", "999999"); r.kind != '-' {
		t.Fatalf("rollback to unknown version: got %c %q, want error", r.kind, r.str)
	}
	if r := c.do("ROLLBACK", "k", "abc"); r.kind != '-' {
		t.Fatalf("rollback with non-integer id: got %c %q, want error", r.kind, r.str)
	}
}

func TestRollbackToTombstoneRejected(t *testing.T) {
	c, _ := newClient(t)
	c.do("SET", "k", "a")
	c.do("DEL", "k")
	ids, _ := parseHistory(t, c.do("HISTORY", "k").arr)
	if r := c.do("ROLLBACK", "k", strconv.FormatUint(ids[len(ids)-1], 10)); r.kind != '-' {
		t.Fatalf("got %c %q, want error", r.kind, r.str)
	}
	expectNull(t, c.do("GET", "k"))
}

// ---------------------------------------------------------------- tiering

func TestEvictThenReadAllCommands(t *testing.T) {
	c, _ := newClient(t)
	c.do("SET", "k", "hello")
	expect(t, c.do("EVICT", "k"), '+', "OK")
	expect(t, c.do("EXISTS", "k"), ':', "1")
	expect(t, c.do("EVICT", "k"), '+', "OK") // evicting twice is harmless
	expect(t, c.do("GET", "k"), '$', "hello")

	// overwrite a key that is currently on disk
	expect(t, c.do("EVICT", "k"), '+', "OK")
	expect(t, c.do("SET", "k", "world"), '+', "OK")
	expect(t, c.do("GET", "k"), '$', "world")
	_, vals := parseHistory(t, c.do("HISTORY", "k").arr)
	if len(vals) != 2 || vals[0] != "hello" || vals[1] != "world" {
		t.Fatalf("history = %v", vals)
	}

	// delete a key that is currently on disk
	expect(t, c.do("EVICT", "k"), '+', "OK")
	expect(t, c.do("DEL", "k"), ':', "1")
	expectNull(t, c.do("GET", "k"))

	expect(t, c.do("EVICT", "never-existed"), '+', "OK")
}

// REGRESSION: the ramKeys counter must equal the number of keys actually
// resident in RAM. If it drifts, the "max keys in RAM" cap silently stops
// working (RAM grows to the whole keyspace while the counter says 5).
func TestRamKeysCounterMatchesReality(t *testing.T) {
	store, err := NewShardStore(filepath.Join(t.TempDir(), "data.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.disk.file.Close()

	actual := func() int64 {
		var n int64
		for i := range store.shards {
			sh := &store.shards[i]
			sh.mu.Lock()
			for _, e := range sh.mem {
				if e.Location == LocationRam {
					n++
				}
			}
			sh.mu.Unlock()
		}
		return n
	}
	check := func(stage string) {
		t.Helper()
		if got, want := store.ramKeys.Load(), actual(); got != want {
			t.Fatalf("after %s: ramKeys counter = %d but %d keys are really in RAM", stage, got, want)
		}
	}

	const n = 60
	for i := 0; i < n; i++ {
		store.Set(fmt.Sprintf("k%d", i), "v")
	}
	check("initial SETs")

	for i := 0; i < n; i += 2 {
		store.Evict(fmt.Sprintf("k%d", i))
	}
	check("evicting half")

	for i := 0; i < n; i += 4 { // SET onto keys that live on disk
		store.Set(fmt.Sprintf("k%d", i), "v2")
	}
	check("SET on disk-resident keys")

	for i := 2; i < n; i += 4 { // GET on keys that live on disk
		store.Get(fmt.Sprintf("k%d", i))
	}
	check("GET on disk-resident keys")

	for i := 0; i < n; i += 3 {
		store.Del(fmt.Sprintf("k%d", i))
	}
	check("DEL")
}

// REGRESSION: a failed SET (disk read error while hydrating) must release the
// shard lock. Otherwise every later operation on that shard hangs forever.
func TestFailedSetDoesNotLeaveShardLocked(t *testing.T) {
	store, err := NewShardStore(filepath.Join(t.TempDir(), "data.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.disk.file.Close()

	store.Set("k", "v")
	if err := store.Evict("k"); err != nil {
		t.Fatal(err)
	}
	// Point the key at bytes that do not exist so the disk read fails.
	sh := &store.shards[fnvHash("k")]
	sh.mu.Lock()
	sh.mem["k"].Stub = DiskStub{Offset: 1 << 30, Length: 16}
	sh.mu.Unlock()

	done := make(chan struct{})
	go func() {
		store.Set("k", "x") // fails: returns 0
		store.Get("k")      // hangs forever if Set leaked the lock
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("shard lock still held after a failed Set (missing Unlock on the error path)")
	}
}

// ---------------------------------------------------------------- protocol

func TestUnknownCommandAndArity(t *testing.T) {
	c, _ := newClient(t)
	if r := c.do("FLUSHALL"); r.kind != '-' || !strings.Contains(r.str, "unknown command") {
		t.Fatalf("got %c %q", r.kind, r.str)
	}
	for _, args := range [][]string{
		{"GET"}, {"GET", "a", "b"},
		{"SET", "a"}, {"SET", "a", "b", "c"},
		{"DEL"}, {"EXISTS"}, {"EXPIRE", "a"}, {"TTL"},
		{"HISTORY"}, {"AS.OF", "a"}, {"ROLLBACK", "a"}, {"EVICT"},
	} {
		if r := c.do(args...); r.kind != '-' {
			t.Fatalf("%v: got %c %q, want an error reply", args, r.kind, r.str)
		}
	}
	if r := c.do("EXPIRE", "a", "soon"); r.kind != '-' {
		t.Fatalf("EXPIRE non-integer: got %c %q", r.kind, r.str)
	}
	if r := c.do("AS.OF", "a", "yesterday"); r.kind != '-' {
		t.Fatalf("AS.OF non-integer: got %c %q", r.kind, r.str)
	}
	// the connection must still be usable after errors
	expect(t, c.do("PING"), '+', "PONG")
}

func TestPipelining(t *testing.T) {
	c, _ := newClient(t)
	const n = 200
	var batch []byte
	for i := 0; i < n; i++ {
		batch = append(batch, encodeTestCmd("SET", fmt.Sprintf("p%d", i), fmt.Sprintf("val%d", i))...)
		batch = append(batch, encodeTestCmd("GET", fmt.Sprintf("p%d", i))...)
	}
	c.conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.conn.Write(batch); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		r, err := readTestReply(c.r)
		if err != nil {
			t.Fatal(err)
		}
		expect(t, r, '+', "OK")
		r, err = readTestReply(c.r)
		if err != nil {
			t.Fatal(err)
		}
		expect(t, r, '$', fmt.Sprintf("val%d", i)) // replies stay in order
	}
}

func TestCommandSplitAcrossTCPWrites(t *testing.T) {
	c, _ := newClient(t)
	raw := encodeTestCmd("SET", "split", "value")
	c.conn.SetDeadline(time.Now().Add(5 * time.Second))
	for i := range raw { // one byte at a time
		if _, err := c.conn.Write(raw[i : i+1]); err != nil {
			t.Fatal(err)
		}
	}
	r, err := readTestReply(c.r)
	if err != nil {
		t.Fatal(err)
	}
	expect(t, r, '+', "OK")
	expect(t, c.do("GET", "split"), '$', "value")
}

func TestMalformedInputClosesConnectionNotServer(t *testing.T) {
	addr, _ := startServer(t)

	bad, err := tcDial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer bad.conn.Close()
	bad.conn.Write([]byte("this is not RESP\r\n"))
	bad.conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := readTestReply(bad.r); err == nil {
		t.Fatal("expected the server to close a connection that sent garbage")
	}

	good, err := tcDial(addr) // server must still be alive for others
	if err != nil {
		t.Fatal(err)
	}
	good.t = t
	defer good.conn.Close()
	expect(t, good.do("PING"), '+', "PONG")
}

func TestParseRESP(t *testing.T) {
	parse := func(s string) ([]string, error) { return parseRESP(bufio.NewReader(strings.NewReader(s))) }

	args, err := parse("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n")
	if err != nil || len(args) != 3 || args[0] != "SET" || args[1] != "k" || args[2] != "hello" {
		t.Fatalf("got %v, %v", args, err)
	}
	for _, bad := range []string{
		"", "PING\r\n", "*x\r\n", "*1\r\n:5\r\n", "*1\r\n$x\r\n", "*1\r\n$4\r\nPI",
	} {
		if _, err := parse(bad); err == nil {
			t.Errorf("parseRESP(%q) = nil error, want error", bad)
		}
	}
}

// REGRESSION: a negative bulk length makes make([]byte, n) panic, and a panic
// in a connection goroutine kills the entire server process. One malformed
// packet from any client must not be able to do that.
func TestParseRESPNegativeBulkLengthDoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("parseRESP panicked on \"$-1\": %v (this would crash the whole server)", r)
		}
	}()
	parseRESP(bufio.NewReader(strings.NewReader("*1\r\n$-1\r\n")))
}

// ---------------------------------------------------------------- concurrency

// Each client owns a private key range, writes it twice, randomly EVICTs keys
// mid-flight (forcing the disk tier regardless of the RAM cap), then reads
// everything back. Any wrong value is lost/corrupted data.
func TestConcurrentClientsDataIntegrity(t *testing.T) {
	addr, _ := startServer(t)
	const clients, keysPer = 32, 150

	var wg sync.WaitGroup
	errs := make(chan string, 1024)
	for id := 0; id < clients; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := tcDial(addr)
			if err != nil {
				errs <- fmt.Sprintf("client %d dial: %v", id, err)
				return
			}
			defer c.conn.Close()
			rng := rand.New(rand.NewSource(int64(id) + 1))
			key := func(i int) string { return fmt.Sprintf("c%d:k%d", id, i) }

			for pass := 1; pass <= 2; pass++ {
				for i := 0; i < keysPer; i++ {
					r, err := c.try("SET", key(i), fmt.Sprintf("v%d-%d-%d", pass, id, i))
					if err != nil || r.kind != '+' {
						errs <- fmt.Sprintf("client %d SET %s: %v %v", id, key(i), r, err)
						return
					}
					if rng.Intn(3) == 0 {
						if _, err := c.try("EVICT", key(rng.Intn(i+1))); err != nil {
							errs <- fmt.Sprintf("client %d EVICT: %v", id, err)
							return
						}
					}
				}
			}
			for i := 0; i < keysPer; i++ {
				want := fmt.Sprintf("v2-%d-%d", id, i)
				r, err := c.try("GET", key(i))
				if err != nil {
					errs <- fmt.Sprintf("client %d GET %s: %v", id, key(i), err)
					return
				}
				if r.null || r.str != want {
					errs <- fmt.Sprintf("client %d GET %s = %q (null=%v), want %q", id, key(i), r.str, r.null, want)
				}
			}
		}(id)
	}
	wg.Wait()
	close(errs)
	reported := 0
	for e := range errs {
		if reported++; reported <= 10 {
			t.Error(e)
		}
	}
	if reported > 10 {
		t.Errorf("... and %d more errors", reported-10)
	}
}

// Many clients hammer ONE key. Whatever interleaving happens, the final value
// must be one that was written, and version ids in HISTORY must be strictly
// increasing (i.e. id assignment and append happen atomically under the lock).
func TestHotKeyContention(t *testing.T) {
	addr, _ := startServer(t)
	const clients, perClient = 16, 200

	var wg sync.WaitGroup
	for id := 0; id < clients; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := tcDial(addr)
			if err != nil {
				t.Errorf("dial: %v", err)
				return
			}
			defer c.conn.Close()
			for i := 0; i < perClient; i++ {
				if _, err := c.try("SET", "hot", fmt.Sprintf("c%d-%d", id, i)); err != nil {
					t.Errorf("SET: %v", err)
					return
				}
				if i%25 == 0 {
					c.try("EVICT", "hot")
				}
			}
		}(id)
	}
	wg.Wait()

	c, err := tcDial(addr)
	if err != nil {
		t.Fatal(err)
	}
	c.t = t
	defer c.conn.Close()

	final := c.do("GET", "hot")
	if final.null || !strings.HasPrefix(final.str, "c") {
		t.Fatalf("final value %q is not one that any client wrote", final.str)
	}
	ids, vals := parseHistory(t, c.do("HISTORY", "hot").arr)
	if len(ids) != 5 {
		t.Fatalf("history length = %d, want 5", len(ids))
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("version ids not increasing: %v", ids)
		}
	}
	if vals[len(vals)-1] != final.str {
		t.Fatalf("latest history value %q != GET value %q", vals[len(vals)-1], final.str)
	}
}
