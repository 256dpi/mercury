package mercury

import "bufio"

// Write will write using the buffered writer and return whether it flushed
// during the write.
func Write(w *bufio.Writer, p []byte) (int, bool, error) {
	// get available bytes
	a := w.Available()

	// write to buffer
	n, err := w.Write(p)

	return n, n > a, err
}
