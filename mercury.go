package mercury

import (
	"bufio"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Writer extends a buffered writer that flushes itself asynchronously. It uses
// a timer to flush the buffered writer it it gets stale. Errors that occur
// during the flush are returned on the next call to Write, Flush or WriteAndFlush.
type Writer struct {
	delay   int64
	queue   int64
	flushes int64
	writer  *bufio.Writer
	timer   *time.Timer
	armed   bool
	err     error
	mutex   sync.Mutex
}

// NewWriter wraps the provided writer and enables buffering and asynchronous
// flushing using the specified maximum delay.
//
// Note: The delay should not be below 1ms to prevent flushing every write
// asynchronously.
func NewWriter(w io.Writer, maxDelay time.Duration) *Writer {
	return newWriter(bufio.NewWriter(w), maxDelay)
}

// NewWriterSize wraps the provided writer and enables buffering and asynchronous
// flushing using the specified maximum delay. This method allows configuration
// of the initial buffer size.
// Note: The delay should not be below 1ms to prevent flushing every write
// asynchronously.
func NewWriterSize(w io.Writer, maxDelay time.Duration, size int) *Writer {
	return newWriter(bufio.NewWriterSize(w, size), maxDelay)
}

func newWriter(w *bufio.Writer, maxDelay time.Duration) *Writer {
	// create writer
	writer := &Writer{
		writer: w,
		delay:  int64(maxDelay),
	}

	// create stopped timer
	writer.timer = time.AfterFunc(time.Second, writer.flush)
	writer.timer.Stop()

	return writer
}

// Write implements the io.Writer interface and writes data to the underlying
// buffered writer and flushes it asynchronously.
func (w *Writer) Write(p []byte) (int, error) {
	return w.write(p, false)
}

// Flush flushes the buffered writer immediately.
func (w *Writer) Flush() error {
	_, err := w.write(nil, true)
	return err
}

// WriteAndFlush writes data to the underlying buffered writer and flushes it
// immediately after writing.
func (w *Writer) WriteAndFlush(p []byte) (int, error) {
	return w.write(p, true)
}

// SetMaxDelay can be used to adjust the maximum delay of asynchronous flushes.
//
// Note: The delay should not be below 1ms to prevent flushing every write
// asynchronously.
func (w *Writer) SetMaxDelay(delay time.Duration) {
	atomic.StoreInt64(&w.delay, int64(delay))
}

// Flushes returns the number asynchronously performed flushes.
func (w *Writer) Flushes() int64 {
	return atomic.LoadInt64(&w.flushes)
}

func (w *Writer) write(p []byte, flush bool) (n int, err error) {
	// acquire mutex
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// clear and return any error from flush
	if w.err != nil {
		err = w.err
		w.err = nil
		return 0, err
	}

	// write data if available
	var flushed bool
	if len(p) > 0 {
		n, flushed, err = Write(w.writer, p)
		if err != nil {
			return n, err
		}
	}

	// get delay
	delay := time.Duration(atomic.LoadInt64(&w.delay))

	// flush immediately if requested or delay is zero
	if flush || delay == 0 {
		err = w.writer.Flush()
		if err != nil {
			return n, err
		}
		flushed = true
	}

	// get buffered
	buffered := w.writer.Buffered()

	// star timer if data is buffered but not armed
	if buffered > 0 && !w.armed {
		w.timer.Reset(delay)
		w.armed = true

		return n, nil
	}

	// stop timer if no data is buffered and armed
	if buffered == 0 && w.armed {
		w.timer.Stop()
		w.armed = false

		return n, nil
	}

	// otherwise reset timer if some data has been flushed when armed
	if flushed && w.armed {
		w.timer.Reset(delay)
	}

	return n, nil
}

func (w *Writer) flush() {
	// return if more than one flush is queued
	n := atomic.LoadInt64(&w.queue)
	if n > 1 {
		return
	}

	// add counter
	atomic.AddInt64(&w.queue, 1)
	defer atomic.AddInt64(&w.queue, -1)

	// acquire mutex
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// set flag
	w.armed = false

	// flush buffer
	err := w.writer.Flush()
	if err != nil && w.err == nil {
		w.err = err
	}

	// count flush
	atomic.AddInt64(&w.flushes, 1)
}
