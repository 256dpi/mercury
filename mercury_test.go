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
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.True(t, w.armed)
	assert.NoError(t, w.err)

	time.Sleep(3 * time.Millisecond)

	assert.Equal(t, []byte{1}, b.Bytes())
	assert.False(t, w.armed)
	assert.NoError(t, w.err)
	assert.Equal(t, int64(1), w.flushes)
}

func TestWriterWriteAndFlush(t *testing.T) {
	b := new(bytes.Buffer)

	w := NewWriter(b, time.Millisecond)
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	n, err := w.WriteAndFlush([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.Equal(t, []byte{1}, b.Bytes())
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	time.Sleep(3 * time.Millisecond)

	assert.Equal(t, int64(0), w.flushes)
}

func TestWriterWriteAndFlushAfterWrite(t *testing.T) {
	b := new(bytes.Buffer)

	w := NewWriter(b, time.Millisecond)
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.True(t, w.armed)
	assert.NoError(t, w.err)

	n, err = w.WriteAndFlush([]byte{2})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.Equal(t, []byte{1, 2}, b.Bytes())
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	time.Sleep(3 * time.Millisecond)

	assert.Equal(t, int64(0), w.flushes)
}

func TestWriterFlush(t *testing.T) {
	b := new(bytes.Buffer)

	w := NewWriter(b, time.Millisecond)
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.True(t, w.armed)
	assert.NoError(t, w.err)

	err = w.Flush()
	assert.NoError(t, err)
	assert.Equal(t, []byte{1}, b.Bytes())
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	time.Sleep(3 * time.Millisecond)

	assert.Equal(t, int64(0), w.flushes)
}

func TestWriterWriteNoDelay(t *testing.T) {
	b := new(bytes.Buffer)

	w := NewWriter(b, 0)
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	time.Sleep(3 * time.Millisecond)

	assert.Equal(t, []byte{1}, b.Bytes())
	assert.False(t, w.armed)
	assert.NoError(t, w.err)
	assert.Equal(t, int64(0), w.flushes)
}

func TestWriterSetMaxDelay(t *testing.T) {
	b := new(bytes.Buffer)

	w := NewWriter(b, time.Minute)
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.True(t, w.armed)
	assert.NoError(t, w.err)

	w.SetMaxDelay(0)

	n, err = w.Write([]byte{2})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	time.Sleep(3 * time.Millisecond)

	assert.Equal(t, []byte{1, 2}, b.Bytes())
	assert.False(t, w.armed)
	assert.NoError(t, w.err)
	assert.Equal(t, int64(0), w.flushes)
}

func TestWriterWriteError(t *testing.T) {
	pr, pw := io.Pipe()
	_ = pr.CloseWithError(errTest)

	w := NewWriterSize(pw, time.Millisecond, 1)
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	n, err := w.Write([]byte{1, 2})
	assert.Equal(t, 0, n)
	assert.Error(t, err)

	assert.Equal(t, int64(0), w.flushes)
}

func TestWriterWriteAndFlushError(t *testing.T) {
	pr, pw := io.Pipe()
	_ = pr.CloseWithError(errTest)

	w := NewWriterSize(pw, time.Millisecond, 1)
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	n, err := w.WriteAndFlush([]byte{1})
	assert.Equal(t, 1, n)
	assert.Error(t, err)

	time.Sleep(3 * time.Millisecond)

	assert.Equal(t, int64(0), w.flushes)
}

func TestWriterWriteAsyncError(t *testing.T) {
	pr, pw := io.Pipe()
	_ = pr.CloseWithError(errTest)

	w := NewWriterSize(pw, time.Millisecond, 2)
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)

	time.Sleep(3 * time.Millisecond)

	n, err = w.Write([]byte{1})
	assert.Equal(t, 0, n)
	assert.Error(t, err)

	assert.Equal(t, int64(1), w.flushes)
}

func benchWriters(b *testing.B, size int64, wrap func(io.Writer) io.Writer) {
	f, err := os.OpenFile(os.DevNull, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	data := make([]byte, size)

	b.ReportAllocs()
	b.SetBytes(size)

	w := wrap(f)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = w.Write(data)
		if err != nil {
			panic(err)
		}

		runtime.Gosched()
	}

	switch w := w.(type) {
	case *Writer:
		b.ReportMetric(float64(w.flushes)/float64(b.N), "flushes/op")
	}
}

func BenchmarkStandard_64(b *testing.B) {
	benchWriters(b, 64, func(writer io.Writer) io.Writer {
		return writer
	})
}

func BenchmarkStandard_256(b *testing.B) {
	benchWriters(b, 256, func(writer io.Writer) io.Writer {
		return writer
	})
}

func BenchmarkStandard_2K(b *testing.B) {
	benchWriters(b, 2048, func(writer io.Writer) io.Writer {
		return writer
	})
}

func BenchmarkStandard_8K(b *testing.B) {
	benchWriters(b, 8192, func(writer io.Writer) io.Writer {
		return writer
	})
}

func BenchmarkBuffered_64(b *testing.B) {
	benchWriters(b, 64, func(writer io.Writer) io.Writer {
		return bufio.NewWriter(writer)
	})
}

func BenchmarkBuffered_256(b *testing.B) {
	benchWriters(b, 256, func(writer io.Writer) io.Writer {
		return bufio.NewWriter(writer)
	})
}

func BenchmarkBuffered_2K(b *testing.B) {
	benchWriters(b, 2048, func(writer io.Writer) io.Writer {
		return bufio.NewWriter(writer)
	})
}

func BenchmarkBuffered_8K(b *testing.B) {
	benchWriters(b, 8192, func(writer io.Writer) io.Writer {
		return bufio.NewWriter(writer)
	})
}

func BenchmarkMercury_64_1us(b *testing.B) {
	benchWriters(b, 64, func(writer io.Writer) io.Writer {
		return NewWriter(writer, time.Microsecond)
	})
}

func BenchmarkMercury_64_1ms(b *testing.B) {
	benchWriters(b, 64, func(writer io.Writer) io.Writer {
		return NewWriter(writer, time.Millisecond)
	})
}

func BenchmarkMercury_256_1us(b *testing.B) {
	benchWriters(b, 256, func(writer io.Writer) io.Writer {
		return NewWriter(writer, time.Microsecond)
	})
}

func BenchmarkMercury_256_1ms(b *testing.B) {
	benchWriters(b, 256, func(writer io.Writer) io.Writer {
		return NewWriter(writer, time.Millisecond)
	})
}

func BenchmarkMercury_2K_1us(b *testing.B) {
	benchWriters(b, 2048, func(writer io.Writer) io.Writer {
		return NewWriter(writer, time.Microsecond)
	})
}

func BenchmarkMercury_2K_1ms(b *testing.B) {
	benchWriters(b, 2048, func(writer io.Writer) io.Writer {
		return NewWriter(writer, time.Millisecond)
	})
}

func BenchmarkMercury_8K_1us(b *testing.B) {
	benchWriters(b, 8192, func(writer io.Writer) io.Writer {
		return NewWriter(writer, time.Microsecond)
	})
}

func BenchmarkMercury_8K_1ms(b *testing.B) {
	benchWriters(b, 8192, func(writer io.Writer) io.Writer {
		return NewWriter(writer, time.Millisecond)
	})
}
