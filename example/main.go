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

var data = bytes.Repeat([]byte{0x0}, 2048)

var bufferedBytes = god.NewCounter("buffered-bytes", func(total int) string {
	return fmt.Sprintf("%.2f MB/s", float64(total)/1000_000)
})

var mercuryBytes = god.NewCounter("mercury-bytes", func(total int) string {
	return fmt.Sprintf("%.2f MB/s", float64(total)/1000_000)
})

func main() {
	god.Init(god.Options{})

	god.Track("goroutines", func() string {
		return strconv.Itoa(runtime.NumGoroutine())
	})

	var flushes uint64
	god.Track("mercury-flushes", func() string {
		f := mercury.GetStats().Flushes
		n := f - flushes
		flushes = f
		return strconv.FormatUint(n, 10)
	})

	for i := 0; i < runtime.NumCPU()/2; i++ {
		go bufferedWriter()
	}

	for i := 0; i < runtime.NumCPU()/2; i++ {
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
