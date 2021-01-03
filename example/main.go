package main

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/256dpi/god"

	"github.com/256dpi/mercury"
)

const maxDelay = time.Millisecond
const bufferSize = 32 * 1024

var data = bytes.Repeat([]byte{0x0}, 256)

var counter = god.NewCounter("data", func(total int) string {
	return fmt.Sprintf("%.2f GB/s", float64(total)/1000_000_000)
})

func main() {
	god.Debug()
	god.Metrics()

	for i := 0; i < runtime.NumCPU(); i++ {
		go writer()
	}

	select {}
}

func writer() {
	fd, err := os.OpenFile(os.DevNull, os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}

	w := mercury.NewWriterSize(fd, maxDelay, bufferSize)
	// w := bufio.NewWriterSize(fd, bufferSize)

	for {
		n, err := w.Write(data)
		if err != nil {
			panic(err)
		}

		counter.Add(n)
	}
}
