package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/256dpi/god"

	"github.com/256dpi/mercury"
)

var data = bytes.Repeat([]byte{0x0}, 256)

var bufferedBytes = god.NewCounter("buffered-bytes", func(total int) string {
	return fmt.Sprintf("%.2f GB/s", float64(total)/1000_000_000)
})

var mercuryBytes = god.NewCounter("mercury-bytes", func(total int) string {
	return fmt.Sprintf("%.2f GB/s", float64(total)/1000_000_000)
})

var mercuryFlushes = god.NewCounter("mercury-flushes", nil)

func main() {
	god.Init(god.Options{})

	god.Track("goroutines", func() string {
		return strconv.Itoa(runtime.NumGoroutine())
	})

	for i := 0; i < runtime.NumCPU()/2; i++ {
		go writer()
	}

	for i := 0; i < runtime.NumCPU()/2; i++ {
		go asyncWriter()
	}

	select {}
}

func writer() {
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

func asyncWriter() {
	fd, err := os.OpenFile(os.DevNull, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}

	w := mercury.NewWriter(fd, time.Millisecond)

	var lf int64
	for {
		n, err := w.Write(data)
		if err != nil {
			panic(err)
		}

		mercuryBytes.Add(n)

		f := w.Flushes()
		mercuryFlushes.Add(int(f - lf))
		lf = f
	}
}
