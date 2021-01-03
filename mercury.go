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
	err     error
	mutex   sync.Mutex
}

// NewWriter wraps the provided writer and enables buffering and asynchronous
// flushing using the specified maximum delay.
func NewWriter(w io.Writer, maxDelay time.Duration) *Writer {
	return &Writer{
		writer: bufio.NewWriter(w),
		delay:  int64(maxDelay),
	}
}

// NewWriterSize wraps the provided writer and enables buffering and asynchronous
// flushing using the specified maximum delay. This method allows configuration
// of the initial buffer size.
func NewWriterSize(w io.Writer, maxDelay time.Duration, size int) *Writer {
	return &Writer{
		writer: bufio.NewWriterSize(w, size),
		delay:  int64(maxDelay),
	}
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
func (w *Writer) SetMaxDelay(delay time.Duration) {
	atomic.StoreInt64(&w.delay, int64(delay))
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

	// prepare flag
	var flushed bool

	// write data if available
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
	}

	// setup timer if data is buffered
	if w.writer.Buffered() > 0 && w.timer == nil {
		w.timer = time.AfterFunc(delay, w.flush)
	}

	// reset timer if data has been flushed
	if flushed && w.timer != nil {
		w.timer.Reset(delay)
	}

	// stop timer if no data is buffered
	if w.writer.Buffered() == 0 && w.timer != nil {
		w.timer.Stop()
		w.timer = nil
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

	// clear timer
	w.timer = nil

	// flush buffer
	err := w.writer.Flush()
	if err != nil && w.err == nil {
		w.err = err
	}

	// count flush
	atomic.AddInt64(&w.flushes, 1)
}
