package main

// Tiered-storage performance report.
//
// Inserts N keys, pushes them to the disk tier, reads them back from disk and
// from RAM, and prints throughput + latency percentiles. Every value read back
// is compared with what was written, so the numbers also prove nothing was
// lost or corrupted.
//
//	go test -run TestTieredStorageReport -v -count=1 -timeout 30m
//
// Knobs (environment variables):
//
//	REO_KEYS=1000000        number of keys            (default 100000)
//	REO_VALUE_SIZE=100      value size in bytes       (default 100)
//	REO_WORKERS=8           goroutines, store-level   (default 8)
//	REO_CONNS=32            TCP connections           (default 32)
//
// A copy of the report is written to reo_report.txt next to this file.
//
// Needs server_test.go in the same package (reuses its TCP client helpers).

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

type phaseResult struct {
	name    string
	ops     int
	bad     int
	elapsed time.Duration
	lat     []time.Duration // sorted
}

func (p phaseResult) pct(q float64) time.Duration {
	if len(p.lat) == 0 {
		return 0
	}
	return p.lat[int(float64(len(p.lat)-1)*q)]
}

func (p phaseResult) avg() time.Duration {
	if len(p.lat) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range p.lat {
		sum += d
	}
	return sum / time.Duration(len(p.lat))
}

func (p phaseResult) opsPerSec() float64 { return float64(p.ops) / p.elapsed.Seconds() }

// measure runs op(worker, key index) for every index in order, split across
// `workers` goroutines, timing each call. op returns false on a wrong result.
func measure(name string, order []int, workers int, op func(w, i int) bool) phaseResult {
	lats := make([][]time.Duration, workers)
	bads := make([]int, workers)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			l := make([]time.Duration, 0, len(order)/workers+1)
			for j := w; j < len(order); j += workers {
				t0 := time.Now()
				ok := op(w, order[j])
				l = append(l, time.Since(t0))
				if !ok {
					bads[w]++
				}
			}
			lats[w] = l
		}(w)
	}
	wg.Wait()
	res := phaseResult{name: name, ops: len(order), elapsed: time.Since(start)}
	for w := 0; w < workers; w++ {
		res.lat = append(res.lat, lats[w]...)
		res.bad += bads[w]
	}
	sort.Slice(res.lat, func(a, b int) bool { return res.lat[a] < res.lat[b] })
	return res
}

func commaInt(n int64) string {
	s := strconv.FormatInt(n, 10)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func fmtDur(d time.Duration) string {
	switch {
	case d >= time.Millisecond:
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%.1fus", float64(d)/float64(time.Microsecond))
	}
}

func tableHeader() string {
	return fmt.Sprintf("%-44s %10s %12s %9s %9s %9s %9s %9s\n",
		"phase", "ops", "ops/sec", "avg", "p50", "p95", "p99", "max")
}

func (p phaseResult) row() string {
	return fmt.Sprintf("%-44s %10s %12s %9s %9s %9s %9s %9s\n",
		p.name, commaInt(int64(p.ops)), commaInt(int64(p.opsPerSec())),
		fmtDur(p.avg()), fmtDur(p.pct(0.50)), fmtDur(p.pct(0.95)), fmtDur(p.pct(0.99)), fmtDur(p.pct(1)))
}

func countLocations(s *ShardedStore) (ram, disk int) {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for _, e := range sh.mem {
			if e.Location == LocationRam {
				ram++
			} else {
				disk++
			}
		}
		sh.mu.Unlock()
	}
	return
}

func TestTieredStorageReport(t *testing.T) {
	n := envInt("REO_KEYS", 100000)
	valSize := envInt("REO_VALUE_SIZE", 100)
	workers := envInt("REO_WORKERS", 8)
	conns := envInt("REO_CONNS", 32)

	key := func(i int) string { return "user:" + strconv.Itoa(i) }
	pad := strings.Repeat("x", valSize)
	val := func(i int) string {
		s := "value-" + strconv.Itoa(i) + "-" + pad
		return s[:valSize]
	}
	rng := rand.New(rand.NewSource(42))
	seq := make([]int, n)
	for i := range seq {
		seq[i] = i
	}
	shuffled := func() []int {
		o := append([]int(nil), seq...)
		rng.Shuffle(len(o), func(a, b int) { o[a], o[b] = o[b], o[a] })
		return o
	}

	var out strings.Builder
	emit := func(format string, args ...any) {
		line := fmt.Sprintf(format, args...)
		out.WriteString(line)
		fmt.Print(line)
	}
	var failures []string
	check := func(p phaseResult) {
		if p.bad > 0 {
			failures = append(failures, fmt.Sprintf("%s: %d wrong/missing results", p.name, p.bad))
		}
	}

	emit("\nREO tiered storage report\n")
	emit("keys=%s  value=%dB  store-level workers=%d  tcp conns=%d\n", commaInt(int64(n)), valSize, workers, conns)
	emit("go=%s  os/arch=%s/%s  cpus=%d\n", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
	emit("RAM cap (REO_MAX_RAM_KEYS): %s keys; colder keys spill to the disk tier\n\n", commaInt(maxKeysInRam))

	// ------------------------------------------------------------ store level
	logPath := filepath.Join(t.TempDir(), "data.log")
	store, err := NewShardStore(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.disk.file.Close()

	emit("== A. Store level (no network) ==\n")
	emit("%s", tableHeader())

	ins := measure("insert (SET, 1 goroutine)", seq, 1, func(_, i int) bool {
		return store.Set(key(i), val(i)) != 0
	})
	emit("%s", ins.row())
	check(ins)

	ramAfter, diskAfter := countLocations(store)
	for i := 0; i < n; i++ { // flush whatever is still in RAM
		store.Evict(key(i))
	}
	ramFlushed, diskAll := countLocations(store)
	fi, _ := os.Stat(logPath)
	var logBytes int64
	if fi != nil {
		logBytes = fi.Size()
	}

	cold1 := measure("cold read from disk (GET, 1 goroutine)", shuffled(), 1, func(_, i int) bool {
		v, ok := store.Get(key(i))
		return ok && v == val(i)
	})
	emit("%s", cold1.row())
	check(cold1)

	warm := measure("warm read from RAM (GET, 1 goroutine)", shuffled(), 1, func(_, i int) bool {
		v, ok := store.Get(key(i))
		return ok && v == val(i)
	})
	emit("%s", warm.row())
	check(warm)

	flush := measure("evict RAM->disk (EVICT, 1 goroutine)", seq, 1, func(_, i int) bool {
		return store.Evict(key(i)) == nil
	})
	emit("%s", flush.row())
	check(flush)

	coldN := measure(fmt.Sprintf("cold read from disk (GET, %d goroutines)", workers), shuffled(), workers, func(_, i int) bool {
		v, ok := store.Get(key(i))
		return ok && v == val(i)
	})
	emit("%s", coldN.row())
	check(coldN)

	emit("\nkeys inserted:                 %s\n", commaInt(int64(n)))
	emit("on disk right after insert:    %s (RAM-resident: %s)\n", commaInt(int64(diskAfter)), commaInt(int64(ramAfter)))
	emit("on disk after full flush:      %s (RAM-resident: %s)\n", commaInt(int64(diskAll)), commaInt(int64(ramFlushed)))
	if n > 0 {
		emit("data.log size:                 %.1f MB  (%.0f bytes/key incl. JSON + version metadata)\n",
			float64(logBytes)/(1<<20), float64(logBytes)/float64(n))
	}
	emit("verified reads (all phases):   %s correct, %d wrong\n\n",
		commaInt(int64(cold1.ops+warm.ops+coldN.ops-cold1.bad-warm.bad-coldN.bad)), cold1.bad+warm.bad+coldN.bad)

	// ------------------------------------------------------------ over TCP
	addr, tstore := startServer(t)
	cl := make([]*tcClient, conns)
	for i := range cl {
		c, err := tcDial(addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		c.t = t
		cl[i] = c
		defer c.conn.Close()
	}

	emit("== B. End to end over TCP (RESP protocol, %d connections, 1 request in flight per connection) ==\n", conns)
	emit("%s", tableHeader())

	tIns := measure(fmt.Sprintf("insert (SET, %d conns)", conns), shuffled(), conns, func(w, i int) bool {
		r, err := cl[w].try("SET", key(i), val(i))
		return err == nil && r.kind == '+'
	})
	emit("%s", tIns.row())
	check(tIns)

	for i := 0; i < n; i++ {
		tstore.Evict(key(i))
	}
	_, tDisk := countLocations(tstore)

	tCold := measure(fmt.Sprintf("cold read from disk (GET, %d conns)", conns), shuffled(), conns, func(w, i int) bool {
		r, err := cl[w].try("GET", key(i))
		return err == nil && !r.null && r.str == val(i)
	})
	emit("%s", tCold.row())
	check(tCold)

	tWarm := measure(fmt.Sprintf("warm read from RAM (GET, %d conns)", conns), shuffled(), conns, func(w, i int) bool {
		r, err := cl[w].try("GET", key(i))
		return err == nil && !r.null && r.str == val(i)
	})
	emit("%s", tWarm.row())
	check(tWarm)
	emit("\nkeys on disk before the TCP cold-read phase: %s of %s\n", commaInt(int64(tDisk)), commaInt(int64(n)))
	emit("(load generator and server share this machine, so network numbers are loopback)\n\n")

	if err := os.WriteFile("reo_report.txt", []byte(out.String()), 0o644); err == nil {
		fmt.Println("report saved to reo_report.txt")
	}
	for _, f := range failures {
		t.Error(f)
	}
}
