package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/256dpi/god"

	"github.com/256dpi/mercury"
)

const writers = 10_000
const maxTimeout = time.Millisecond
const maxDelay = 500 * time.Millisecond
const bufferSize = 32 * 1024

var data = bytes.Repeat([]byte{0x0}, 256)

var counter = god.NewCounter("data", func(total int) string {
	return fmt.Sprintf("%.2f MB/s", float64(total)/1024/1024)
})

func main() {
	god.Debug()
	god.Metrics()

	for i := 0; i < writers; i++ {
		go writer()
	}

	select {}
}

func writer() {
	fd, err := os.OpenFile("/dev/null", os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}

	w := mercury.NewWriterSize(fd, maxDelay, bufferSize)

	for {
		time.Sleep(time.Duration(rand.Intn(int(maxTimeout))))

		n, err := w.Write(data)
		if err != nil {
			panic(err)
		}

		counter.Add(n)
	}
}
