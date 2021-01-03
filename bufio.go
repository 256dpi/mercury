package mercury

import "bufio"

func Write(w *bufio.Writer, p []byte) (int, bool, error) {
	// get available bytes
	a := w.Available()

	// write to buffer
	n, err := w.Write(p)

	return n, n > a, err
}
