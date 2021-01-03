package mercury

import (
	"bufio"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWrite(t *testing.T) {
	w := bufio.NewWriterSize(ioutil.Discard, 32)

	n, flushed, err := Write(w, make([]byte, 10))
	assert.NoError(t, err)
	assert.Equal(t, 10, n)
	assert.False(t, flushed)
	assert.Equal(t, 10, w.Buffered())

	n, flushed, err = Write(w, make([]byte, 22))
	assert.NoError(t, err)
	assert.Equal(t, 22, n)
	assert.False(t, flushed)
	assert.Equal(t, 32, w.Buffered())

	n, flushed, err = Write(w, make([]byte, 7))
	assert.NoError(t, err)
	assert.Equal(t, 7, n)
	assert.True(t, flushed)
	assert.Equal(t, 7, w.Buffered())

	n, flushed, err = Write(w, make([]byte, 42))
	assert.NoError(t, err)
	assert.Equal(t, 42, n)
	assert.True(t, flushed)
	assert.Equal(t, 17, w.Buffered())

	err = w.Flush()
	assert.NoError(t, err)
	assert.Equal(t, 0, w.Buffered())

	n, flushed, err = Write(w, make([]byte, 42))
	assert.NoError(t, err)
	assert.Equal(t, 42, n)
	assert.True(t, flushed)
	assert.Equal(t, 0, w.Buffered())
}
