package mercury

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var errTest = errors.New("test")

func TestGetStats(t *testing.T) {
	GetStats()
}

func TestWriterWrite(t *testing.T) {
	s := GetStats()
	b := new(bytes.Buffer)

	w := NewWriter(b, time.Millisecond)
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.True(t, w.armed)
	assert.NoError(t, w.err)

	n, err = w.Write([]byte{2})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.True(t, w.armed)
	assert.NoError(t, w.err)

	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, []byte{1, 2}, b.Bytes())
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	time.Sleep(10 * time.Millisecond)
	s = GetStats().Sub(s)
	assert.Equal(t, Stats{Initiated: 1, Executed: 1}, s)
}

func TestWriterWriteAndFlush(t *testing.T) {
	s := GetStats()
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

	time.Sleep(10 * time.Millisecond)
	s = GetStats().Sub(s)
	assert.Equal(t, Stats{}, s)
}

func TestWriterWriteAndFlushAfterWrite(t *testing.T) {
	s := GetStats()
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

	time.Sleep(10 * time.Millisecond)
	s = GetStats().Sub(s)
	assert.Equal(t, Stats{Initiated: 1, Cancelled: 1}, s)
}

func TestWriterFlush(t *testing.T) {
	s := GetStats()
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

	time.Sleep(10 * time.Millisecond)
	s = GetStats().Sub(s)
	assert.Equal(t, Stats{Initiated: 1, Cancelled: 1}, s)
}

func TestWriterWriteNoDelay(t *testing.T) {
	s := GetStats()
	b := new(bytes.Buffer)

	w := NewWriter(b, 0)
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, []byte{1}, b.Bytes())
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	time.Sleep(10 * time.Millisecond)
	s = GetStats().Sub(s)
	assert.Equal(t, Stats{}, s)
}

func TestWriterSetMaxDelay(t *testing.T) {
	s := GetStats()
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

	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, []byte{1, 2}, b.Bytes())
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	time.Sleep(10 * time.Millisecond)
	s = GetStats().Sub(s)
	assert.Equal(t, Stats{Initiated: 1, Cancelled: 1}, s)
}

func TestWriterExtendedFlush(t *testing.T) {
	s := GetStats()
	b := new(bytes.Buffer)

	w := NewWriterSize(b, time.Millisecond, 2)
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.True(t, w.armed)
	assert.NoError(t, w.err)

	n, err = w.Write([]byte{2, 3})
	assert.Equal(t, 2, n)
	assert.NoError(t, err)
	assert.True(t, w.armed)
	assert.NoError(t, w.err)

	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, []byte{1, 2, 3}, b.Bytes())
	assert.False(t, w.armed)
	assert.NoError(t, w.err)

	time.Sleep(10 * time.Millisecond)
	s = GetStats().Sub(s)
	assert.Equal(t, Stats{Initiated: 1, Extended: 1, Executed: 1}, s)
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

	time.Sleep(10 * time.Millisecond)

	n, err = w.Write([]byte{1})
	assert.Equal(t, 0, n)
	assert.Error(t, err)
}

func benchWriters(b *testing.B, size int64, wrap func(io.Writer) io.Writer) {
	f, err := os.OpenFile(os.DevNull, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	data := make([]byte, size)

	s := GetStats()

	b.ReportAllocs()
	b.SetBytes(size)

	w := wrap(f)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = w.Write(data)
		if err != nil {
			panic(err)
		}
	}

	s = GetStats().Sub(s)
	b.ReportMetric(float64(s.Executed), "flushes")
}

func BenchmarkStandard_32(b *testing.B) {
	benchWriters(b, 32, func(writer io.Writer) io.Writer {
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

func BenchmarkStandard_16K(b *testing.B) {
	benchWriters(b, 16384, func(writer io.Writer) io.Writer {
		return writer
	})
}

func BenchmarkBuffered_32(b *testing.B) {
	benchWriters(b, 32, func(writer io.Writer) io.Writer {
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

func BenchmarkBuffered_16K(b *testing.B) {
	benchWriters(b, 16384, func(writer io.Writer) io.Writer {
		return bufio.NewWriter(writer)
	})
}

func BenchmarkMercury_32_10us(b *testing.B) {
	benchWriters(b, 32, func(writer io.Writer) io.Writer {
		return NewWriter(writer, 10*time.Microsecond)
	})
}

func BenchmarkMercury_32_1ms(b *testing.B) {
	benchWriters(b, 32, func(writer io.Writer) io.Writer {
		return NewWriter(writer, time.Millisecond)
	})
}

func BenchmarkMercury_256_10us(b *testing.B) {
	benchWriters(b, 256, func(writer io.Writer) io.Writer {
		return NewWriter(writer, 10*time.Microsecond)
	})
}

func BenchmarkMercury_256_1ms(b *testing.B) {
	benchWriters(b, 256, func(writer io.Writer) io.Writer {
		return NewWriter(writer, time.Millisecond)
	})
}

func BenchmarkMercury_2K_10us(b *testing.B) {
	benchWriters(b, 2048, func(writer io.Writer) io.Writer {
		return NewWriter(writer, 10*time.Microsecond)
	})
}

func BenchmarkMercury_2K_1ms(b *testing.B) {
	benchWriters(b, 2048, func(writer io.Writer) io.Writer {
		return NewWriter(writer, time.Millisecond)
	})
}

func BenchmarkMercury_16K_10us(b *testing.B) {
	benchWriters(b, 16384, func(writer io.Writer) io.Writer {
		return NewWriter(writer, 10*time.Microsecond)
	})
}

func BenchmarkMercury_16K_1ms(b *testing.B) {
	benchWriters(b, 16384, func(writer io.Writer) io.Writer {
		return NewWriter(writer, time.Millisecond)
	})
}

func BenchmarkDiscard_32_1ns(b *testing.B) {
	benchWriters(b, 32, func(writer io.Writer) io.Writer {
		return NewWriter(ioutil.Discard, time.Nanosecond)
	})
}
