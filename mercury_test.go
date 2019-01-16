package mercury

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var errTest = errors.New("test")

func TestWriterWrite(t *testing.T) {
	b := new(bytes.Buffer)

	w := NewWriter(b, time.Millisecond)
	assert.Nil(t, w.t)
	assert.NoError(t, w.e)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.NotNil(t, w.t)
	assert.NoError(t, w.e)

	time.Sleep(3 * time.Millisecond)

	assert.Equal(t, []byte{1}, b.Bytes())
	assert.Nil(t, w.t)
	assert.NoError(t, w.e)
}

func TestWriterWriteAndFlush(t *testing.T) {
	b := new(bytes.Buffer)

	w := NewWriter(b, time.Millisecond)
	assert.Nil(t, w.t)
	assert.NoError(t, w.e)

	n, err := w.WriteAndFlush([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.Equal(t, []byte{1}, b.Bytes())
	assert.Nil(t, w.t)
	assert.NoError(t, w.e)
}

func TestWriterWriteAndFlushAfterWrite(t *testing.T) {
	b := new(bytes.Buffer)

	w := NewWriter(b, time.Millisecond)
	assert.Nil(t, w.t)
	assert.NoError(t, w.e)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.NotNil(t, w.t)
	assert.NoError(t, w.e)

	n, err = w.WriteAndFlush([]byte{2})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.Equal(t, []byte{1, 2}, b.Bytes())
	assert.Nil(t, w.t)
	assert.NoError(t, w.e)
}

func TestWriterFlush(t *testing.T) {
	b := new(bytes.Buffer)

	w := NewWriter(b, time.Millisecond)
	assert.Nil(t, w.t)
	assert.NoError(t, w.e)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.NotNil(t, w.t)
	assert.NoError(t, w.e)

	err = w.Flush()
	assert.NoError(t, err)
	assert.Equal(t, []byte{1}, b.Bytes())
	assert.Nil(t, w.t)
	assert.NoError(t, w.e)
}

func TestWriterWriteError(t *testing.T) {
	pr, pw := io.Pipe()
	_ = pr.CloseWithError(errTest)

	w := NewWriterSize(pw, time.Millisecond, 1)
	assert.Nil(t, w.t)
	assert.NoError(t, w.e)

	n, err := w.Write([]byte{1, 2})
	assert.Equal(t, 0, n)
	assert.Error(t, err)
}

func TestWriterWriteAndFlushError(t *testing.T) {
	pr, pw := io.Pipe()
	_ = pr.CloseWithError(errTest)

	w := NewWriterSize(pw, time.Millisecond, 1)
	assert.Nil(t, w.t)
	assert.NoError(t, w.e)

	n, err := w.WriteAndFlush([]byte{1})
	assert.Equal(t, 1, n)
	assert.Error(t, err)
}

func TestWriterWriteAsyncError(t *testing.T) {
	pr, pw := io.Pipe()
	_ = pr.CloseWithError(errTest)

	w := NewWriterSize(pw, time.Millisecond, 2)
	assert.Nil(t, w.t)
	assert.NoError(t, w.e)

	n, err := w.Write([]byte{1})
	assert.Equal(t, 1, n)
	assert.NoError(t, err)

	time.Sleep(3 * time.Millisecond)

	n, err = w.Write([]byte{1})
	assert.Equal(t, 0, n)
	assert.Error(t, err)
}

var data64 = bytes.Repeat([]byte{1}, 64)

func BenchmarkStandardWriter64(b *testing.B) {
	f, err := os.OpenFile(os.DevNull, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}

	for i := 0; i < b.N; i++ {
		_, err = f.Write(data64)
		if err != nil {
			panic(err)
		}

		time.Sleep(1)
	}
}

func BenchmarkBufferedWriter64(b *testing.B) {
	f, err := os.OpenFile(os.DevNull, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}

	w := bufio.NewWriter(f)

	for i := 0; i < b.N; i++ {
		_, err = w.Write(data64)
		if err != nil {
			panic(err)
		}

		time.Sleep(1)
	}
}

func BenchmarkMercuryWriter64(b *testing.B) {
	f, err := os.OpenFile(os.DevNull, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}

	w := NewWriter(f, time.Millisecond)

	for i := 0; i < b.N; i++ {
		_, err = w.Write(data64)
		if err != nil {
			panic(err)
		}

		time.Sleep(1)
	}
}

var data64k = bytes.Repeat([]byte{1}, 64*1024)

func BenchmarkStandardWriter64K(b *testing.B) {
	f, err := os.OpenFile(os.DevNull, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}

	for i := 0; i < b.N; i++ {
		_, err = f.Write(data64k)
		if err != nil {
			panic(err)
		}

		time.Sleep(1)
	}
}

func BenchmarkBufferedWriter64K(b *testing.B) {
	f, err := os.OpenFile(os.DevNull, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}

	w := bufio.NewWriter(f)

	for i := 0; i < b.N; i++ {
		_, err = w.Write(data64k)
		if err != nil {
			panic(err)
		}

		time.Sleep(1)
	}
}

func BenchmarkMercuryWriter64K(b *testing.B) {
	f, err := os.OpenFile(os.DevNull, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}

	w := NewWriter(f, time.Millisecond)

	for i := 0; i < b.N; i++ {
		_, err = w.Write(data64k)
		if err != nil {
			panic(err)
		}

		time.Sleep(1)
	}
}
