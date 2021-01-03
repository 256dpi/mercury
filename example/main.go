package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/256dpi/god"

	"github.com/256dpi/mercury"
)

var writers = flag.Int("writers", runtime.NumCPU(), "the number of writers")

var data = bytes.Repeat([]byte{0x0}, 2048)

var bufferedBytes = god.NewCounter("buffered-bytes", func(total int) string {
	return fmt.Sprintf("%.2f MB/s", float64(total)/1000_000)
})

var mercuryBytes = god.NewCounter("mercury-bytes", func(total int) string {
	return fmt.Sprintf("%.2f MB/s", float64(total)/1000_000)
})

func main() {
	flag.Parse()

	fmt.Printf("running with %d writers writers...\n", *writers)

	god.Init(god.Options{})

	god.Track("goroutines", func() string {
		return strconv.Itoa(runtime.NumGoroutine())
	})

	var stats mercury.Stats
	god.Track("mercury-stats", func() string {
		s := mercury.GetStats()
		d := s.Sub(stats)
		stats = s
		return fmt.Sprintf("%d/%d", d.Executed, d.Skipped)
	})

	for i := 0; i < *writers/2; i++ {
		go bufferedWriter()
	}

	for i := 0; i < *writers/2; i++ {
		go mercuryWriter()
	}

	select {}
}

func bufferedWriter() {
	fd, err := os.OpenFile(os.DevNull, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}

	w := bufio.NewWriter(fd)

	for {
		n, err := w.Write(data)
		if err != nil {
			panic(err)
		}

		bufferedBytes.Add(n)
	}
}

func mercuryWriter() {
	fd, err := os.OpenFile(os.DevNull, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}

	w := mercury.NewWriter(fd, time.Millisecond)

	for {
		n, err := w.Write(data)
		if err != nil {
			panic(err)
		}

		mercuryBytes.Add(n)
	}
}
