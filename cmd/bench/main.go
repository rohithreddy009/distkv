// bench load-tests a running DistKV cluster and reports throughput and
// latency percentiles.
//
//	go run ./cmd/bench -cluster localhost:7001,localhost:7002,localhost:7003 \
//	    -writers 16 -readers 16 -duration 15s -keys 10000 -valsize 128
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rohithreddy/distkv/client"
)

func main() {
	cluster := flag.String("cluster", "localhost:7001,localhost:7002,localhost:7003", "KV addresses")
	writers := flag.Int("writers", 16, "concurrent write workers")
	readers := flag.Int("readers", 16, "concurrent read workers")
	duration := flag.Duration("duration", 15*time.Second, "benchmark duration")
	keys := flag.Int("keys", 10000, "keyspace size")
	valsize := flag.Int("valsize", 128, "value size in bytes")
	flag.Parse()

	addrs := strings.Split(*cluster, ",")
	value := make([]byte, *valsize)
	rand.Read(value)

	// Preload some keys so reads hit.
	pre := client.New(addrs)
	ctx := context.Background()
	fmt.Fprintf(os.Stderr, "preloading %d keys...\n", min(*keys, 1000))
	for i := 0; i < min(*keys, 1000); i++ {
		if err := pre.Put(ctx, key(i, *keys), value); err != nil {
			fmt.Fprintf(os.Stderr, "preload failed: %v\n", err)
			os.Exit(1)
		}
	}

	var mu sync.Mutex
	var writeLat, readLat []time.Duration
	var writeErr, readErr int

	stop := time.Now().Add(*duration)
	var wg sync.WaitGroup

	for w := 0; w < *writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := client.New(addrs)
			rng := rand.New(rand.NewSource(rand.Int63()))
			var lats []time.Duration
			errs := 0
			for time.Now().Before(stop) {
				k := key(rng.Intn(*keys), *keys)
				t0 := time.Now()
				err := c.Put(ctx, k, value)
				if err != nil {
					errs++
					continue
				}
				lats = append(lats, time.Since(t0))
			}
			mu.Lock()
			writeLat = append(writeLat, lats...)
			writeErr += errs
			mu.Unlock()
		}()
	}

	for r := 0; r < *readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := client.New(addrs)
			rng := rand.New(rand.NewSource(rand.Int63()))
			var lats []time.Duration
			errs := 0
			for time.Now().Before(stop) {
				k := key(rng.Intn(min(*keys, 1000)), *keys)
				t0 := time.Now()
				_, _, err := c.Get(ctx, k)
				if err != nil {
					errs++
					continue
				}
				lats = append(lats, time.Since(t0))
			}
			mu.Lock()
			readLat = append(readLat, lats...)
			readErr += errs
			mu.Unlock()
		}()
	}

	wg.Wait()

	fmt.Printf("DistKV benchmark: %d writers, %d readers, %v, %dB values, %d-key space\n\n",
		*writers, *readers, *duration, *valsize, *keys)
	report("writes", writeLat, writeErr, *duration)
	report("reads ", readLat, readErr, *duration)
}

func key(i, space int) string { return fmt.Sprintf("bench-%08d", i%space) }

func report(name string, lats []time.Duration, errs int, dur time.Duration) {
	if len(lats) == 0 {
		fmt.Printf("%s: no successful ops (%d errors)\n", name, errs)
		return
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pct := func(p float64) time.Duration {
		i := int(p * float64(len(lats)-1))
		return lats[i]
	}
	fmt.Printf("%s: %8d ops  %9.0f ops/s  p50=%-8v p90=%-8v p99=%-8v max=%-8v errors=%d\n",
		name, len(lats), float64(len(lats))/dur.Seconds(),
		pct(0.50).Round(10*time.Microsecond),
		pct(0.90).Round(10*time.Microsecond),
		pct(0.99).Round(10*time.Microsecond),
		lats[len(lats)-1].Round(10*time.Microsecond),
		errs)
}
