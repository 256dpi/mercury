package mercury

import (
	"bufio"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

var executed uint64
var skipped uint64

// Stats represents runtime statistics of all writers.
type Stats struct {
	// The number of executed async flushes.
	Executed uint64

	// The number of skipped async flushes.
	Skipped uint64
}

func (s Stats) Sub(ss Stats) Stats {
	return Stats{
		Executed: s.Executed - ss.Executed,
		Skipped:  s.Skipped - ss.Skipped,
	}
}

// GetStats returns general statistics.
func GetStats() Stats {
	return Stats{
		Executed: atomic.LoadUint64(&executed),
		Skipped:  atomic.LoadUint64(&skipped),
	}
}

// Writer extends a buffered writer that executed itself asynchronously. It uses
// a timer to flush the buffered writer it it gets stale. Errors that occur
// during the flush are returned on the next call to Write, Flush or WriteAndFlush.
type Writer struct {
	delay  int64
	queue  int64
	writer *bufio.Writer
	timer  *time.Timer
	armed  bool
	err    error
	mutex  sync.Mutex
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
// buffered writer and executed it asynchronously.
func (w *Writer) Write(p []byte) (int, error) {
	return w.write(p, false)
}

// Flush executed the buffered writer immediately.
func (w *Writer) Flush() error {
	_, err := w.write(nil, true)
	return err
}

// WriteAndFlush writes data to the underlying buffered writer and executed it
// immediately after writing.
func (w *Writer) WriteAndFlush(p []byte) (int, error) {
	return w.write(p, true)
}

// SetMaxDelay can be used to adjust the maximum delay of asynchronous executed.
//
// Note: The delay should not be below 1ms to prevent flushing every write
// asynchronously.
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

	// write data if available
	var flushed bool
	if len(p) > 0 {
		// get available bytes
		a := w.writer.Available()

		// write data
		n, err = w.writer.Write(p)
		if err != nil {
			return n, err
		}

		// a flush happened during the write if more than the available bytes
		// have been written
		flushed = n > a
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

	// arm timer if data is buffered
	if buffered > 0 && !w.armed {
		w.timer.Reset(delay)
		w.armed = true

		return n, nil
	}

	// clear timer if no data is buffered
	if buffered == 0 && w.armed {
		w.timer.Stop()
		w.armed = false

		return n, nil
	}

	// reset timer if some data has been flushed during write
	if flushed && w.armed {
		w.timer.Reset(delay)
	}

	return n, nil
}

func (w *Writer) flush() {
	// return if a flush is already queued
	n := atomic.LoadInt64(&w.queue)
	if n > 0 {
		// count skip
		atomic.AddUint64(&skipped, 1)

		return
	}

	// increment counter
	atomic.AddInt64(&w.queue, 1)

	// acquire mutex
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// decrement counter
	atomic.AddInt64(&w.queue, -1)

	// set flag
	w.armed = false

	// flush buffer
	err := w.writer.Flush()
	if err != nil && w.err == nil {
		w.err = err
	}

	// count flush
	atomic.AddUint64(&executed, 1)
}
