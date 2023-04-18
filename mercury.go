package mercury

import (
	"bufio"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

var initiated uint64
var executed uint64
var extended uint64
var cancelled uint64

// Stats holds runtime statistics.
type Stats struct {
	Initiated uint64
	Executed  uint64
	Extended  uint64
	Cancelled uint64
}

// Sub will return the difference of the two stats objects.
func (s Stats) Sub(ss Stats) Stats {
	return Stats{
		Initiated: s.Initiated - ss.Initiated,
		Executed:  s.Executed - ss.Executed,
		Extended:  s.Extended - ss.Extended,
		Cancelled: s.Cancelled - ss.Cancelled,
	}
}

// GetStats returns runtime statistics.
func GetStats() Stats {
	return Stats{
		Initiated: atomic.LoadUint64(&initiated),
		Executed:  atomic.LoadUint64(&executed),
		Extended:  atomic.LoadUint64(&extended),
		Cancelled: atomic.LoadUint64(&cancelled),
	}
}

// Writer extends a buffered writer that flushes itself asynchronously. It uses
// a timer to flush the buffered writer if it gets stale. Errors that occur
// during and asynchronous flush are returned on the next call to Write, Flush
// or WriteAndFlush.
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
//
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
		atomic.AddUint64(&initiated, 1)
		w.timer.Reset(delay)
		w.armed = true

		return n, nil
	}

	// clear timer if no data is buffered and the timer has not yet fired
	if buffered == 0 && w.armed {
		if w.timer.Stop() {
			atomic.AddUint64(&cancelled, 1)
			w.armed = false
		}

		return n, nil
	}

	// reset timer if data has been flushed and the timer has not yet fired
	if flushed && w.armed {
		if w.timer.Stop() {
			atomic.AddUint64(&extended, 1)
			w.timer.Reset(delay)
		}
	}

	return n, nil
}

func (w *Writer) flush() {
	// count flush
	atomic.AddUint64(&executed, 1)

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
}
