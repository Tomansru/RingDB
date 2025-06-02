package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"runtime"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/tomansru/ringdb/pkg/ringdb"
)

func main() {
	const filePath = "./vlog.vlog"
	const maxSize = 4 * humanize.GiByte

	db, err := ringdb.NewRingDB(filePath, maxSize, 64, 100*time.Microsecond)
	if err != nil {
		fmt.Printf("Error creating RingDB: %v\n", err)
		return
	}
	defer db.Close()

	var now time.Time
	// Пишем несколько раз
	for i := 0; i < 1_200_000; i++ {
		now = time.Now()

		key := fmt.Sprintf("key_%06d", i)
		err := db.Upsert(&ringdb.Entity{
			Key:   key,
			Value: GenJunk40KBPattern(),
		})
		if err != nil {
			fmt.Printf("Error upserting key %s: %v\n", key, err)
			continue
		}

		fmt.Printf("%03d: elapsed: %s\n", i, time.Since(now))
	}

	now = time.Now()
	res, _ := db.ReadPrefix("key_100")
	fmt.Printf("%03d: elapsed: %s\n", len(res), time.Since(now))

	now = time.Now()
	res, _ = db.ReadPrefix("key_400")
	fmt.Printf("%03d: elapsed: %s\n", len(res), time.Since(now))

	m := db.VlogMetrics()
	data, _ := json.Marshal(&m)
	fmt.Printf("Metrics: %s\n", data)

	mem := runtime.MemStats{}
	runtime.ReadMemStats(&mem)
	hmem := MemStats{
		Alloc:       humanize.Bytes(mem.Alloc),
		Sys:         humanize.Bytes(mem.Sys),
		Mallocs:     mem.Mallocs,
		Frees:       mem.Frees,
		LiveObjects: mem.Mallocs - mem.Frees,
		HeapAlloc:   humanize.Bytes(mem.HeapAlloc),
	}
	data, _ = json.Marshal(&hmem)
	fmt.Printf("Memory usage: %s\n", data)
	fmt.Println("Successfully written data to vlog file")
}

// GenJunk40KBPattern returns a 40 KB []byte of repeating pattern (fast, reproducible).
func GenJunk40KBPattern() []byte {
	const size = 4 * 1024
	pattern := []byte("junk")
	buf := bytes.Repeat(pattern, size/len(pattern))
	buf = append(buf, pattern[:size%len(pattern)]...)
	return buf
}

type MemStats struct {
	Alloc       string
	Sys         string
	Mallocs     uint64
	Frees       uint64
	LiveObjects uint64
	HeapAlloc   string
}
