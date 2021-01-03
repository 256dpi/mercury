package mercury

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var errTest = errors.New("test")

func TestWriterWrite(t *testing.T) {
	b := new(bytes.Buffer)

	w := NewWriter(b, time.Millisecond)
	assert.Nil(t, w.timer)
	assert.NoError(t, w.err)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.NotNil(t, w.timer)
	assert.NoError(t, w.err)

	time.Sleep(3 * time.Millisecond)

	assert.Equal(t, []byte{1}, b.Bytes())
	assert.Nil(t, w.timer)
	assert.NoError(t, w.err)
}

func TestWriterWriteAndFlush(t *testing.T) {
	b := new(bytes.Buffer)

	w := NewWriter(b, time.Millisecond)
	assert.Nil(t, w.timer)
	assert.NoError(t, w.err)

	n, err := w.WriteAndFlush([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.Equal(t, []byte{1}, b.Bytes())
	assert.Nil(t, w.timer)
	assert.NoError(t, w.err)
}

func TestWriterWriteAndFlushAfterWrite(t *testing.T) {
	b := new(bytes.Buffer)

	w := NewWriter(b, time.Millisecond)
	assert.Nil(t, w.timer)
	assert.NoError(t, w.err)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.NotNil(t, w.timer)
	assert.NoError(t, w.err)

	n, err = w.WriteAndFlush([]byte{2})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.Equal(t, []byte{1, 2}, b.Bytes())
	assert.Nil(t, w.timer)
	assert.NoError(t, w.err)
}

func TestWriterFlush(t *testing.T) {
	b := new(bytes.Buffer)

	w := NewWriter(b, time.Millisecond)
	assert.Nil(t, w.timer)
	assert.NoError(t, w.err)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.NotNil(t, w.timer)
	assert.NoError(t, w.err)

	err = w.Flush()
	assert.NoError(t, err)
	assert.Equal(t, []byte{1}, b.Bytes())
	assert.Nil(t, w.timer)
	assert.NoError(t, w.err)
}

func TestWriterWriteNoDelay(t *testing.T) {
	b := new(bytes.Buffer)

	w := NewWriter(b, 0)
	assert.Nil(t, w.timer)
	assert.NoError(t, w.err)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.Nil(t, w.timer)
	assert.NoError(t, w.err)

	assert.Equal(t, []byte{1}, b.Bytes())
	assert.Nil(t, w.timer)
	assert.NoError(t, w.err)
}

func TestWriterSetMaxDelay(t *testing.T) {
	b := new(bytes.Buffer)

	w := NewWriter(b, time.Minute)
	assert.Nil(t, w.timer)
	assert.NoError(t, w.err)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.NotNil(t, w.timer)
	assert.NoError(t, w.err)

	w.SetMaxDelay(0)

	n, err = w.Write([]byte{2})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.Nil(t, w.timer)
	assert.NoError(t, w.err)

	assert.Equal(t, []byte{1, 2}, b.Bytes())
	assert.Nil(t, w.timer)
	assert.NoError(t, w.err)
}

func TestWriterWriteError(t *testing.T) {
	pr, pw := io.Pipe()
	_ = pr.CloseWithError(errTest)

	w := NewWriterSize(pw, time.Millisecond, 1)
	assert.Nil(t, w.timer)
	assert.NoError(t, w.err)

	n, err := w.Write([]byte{1, 2})
	assert.Equal(t, 0, n)
	assert.Error(t, err)
}

func TestWriterWriteAndFlushError(t *testing.T) {
	pr, pw := io.Pipe()
	_ = pr.CloseWithError(errTest)

	w := NewWriterSize(pw, time.Millisecond, 1)
	assert.Nil(t, w.timer)
	assert.NoError(t, w.err)

	n, err := w.WriteAndFlush([]byte{1})
	assert.Equal(t, 1, n)
	assert.Error(t, err)
}

func TestWriterWriteAsyncError(t *testing.T) {
	pr, pw := io.Pipe()
	_ = pr.CloseWithError(errTest)

	w := NewWriterSize(pw, time.Millisecond, 2)
	assert.Nil(t, w.timer)
	assert.NoError(t, w.err)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)

	time.Sleep(3 * time.Millisecond)

	n, err = w.Write([]byte{1})
	assert.Equal(t, 0, n)
	assert.Error(t, err)
}

func benchWriters(b *testing.B, size int64) {
	f, err := os.OpenFile(os.DevNull, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	data := make([]byte, size)

	b.Run("Standard", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(size)

		for i := 0; i < b.N; i++ {
			_, err = f.Write(data)
			if err != nil {
				panic(err)
			}

			runtime.Gosched()
		}
	})

	b.Run("Buffered", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(size)

		w := bufio.NewWriter(f)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err = w.Write(data)
			if err != nil {
				panic(err)
			}

			runtime.Gosched()
		}
	})

	b.Run("Mercury-1us", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(size)

		w := NewWriter(f, time.Microsecond)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err = w.Write(data)
			if err != nil {
				panic(err)
			}

			runtime.Gosched()
		}

		b.ReportMetric(float64(w.flushes)/float64(b.N), "flushes/op")
	})

	b.Run("Mercury-1ms", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(size)

		w := NewWriter(f, time.Millisecond)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err = w.Write(data)
			if err != nil {
				panic(err)
			}

			runtime.Gosched()
		}

		b.ReportMetric(float64(w.flushes)/float64(b.N), "flushes/op")
	})
}

func Benchmark64(b *testing.B) {
	benchWriters(b, 64)
}

func Benchmark256(b *testing.B) {
	benchWriters(b, 256)
}

func Benchmark2K(b *testing.B) {
	benchWriters(b, 2048)
}

func Benchmark8K(b *testing.B) {
	benchWriters(b, 8192)
}
