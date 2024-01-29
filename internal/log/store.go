package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	File *os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())

	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	// Lock the mutex to ensure exclusive access to shared resources.
	s.mu.Lock()
	defer s.mu.Unlock() // Ensure unlocking the mutex when the function exits.

	// Store the current size of the file as the position where the new data will be appended.
	pos = s.size

	// Write the length of the data slice 'p' as an 8-byte unsigned integer in big-endian format
	// to the buffer.
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err // Return 0 values and the encountered error if writing the length fails.
	}

	// Write the actual data 'p' to the buffer.
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err // Return 0 values and the encountered error if writing data fails.
	}

	// Add the width of the length field to 'w' to account for the length prefix.
	w += lenWidth

	// Update the size of the file with the new total size.
	s.size += uint64(w)

	// Return the number of bytes written ('w'), the position where data was appended ('pos'), and no error.
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	// Lock the mutex to ensure exclusive access to shared resources.
	s.mu.Lock()
	defer s.mu.Unlock() // Ensure unlocking the mutex when the function exits.

	// Flush the buffer to ensure all buffered data is written to the underlying file.
	if err := s.buf.Flush(); err != nil {
		return nil, err // Return nil slice and the encountered error if flushing the buffer fails.
	}

	// Read the length of the data from the file at the specified position.
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err // Return nil slice and the encountered error if reading the length fails.
	}

	// Convert the read length bytes to an unsigned integer using big-endian encoding.
	dataLength := enc.Uint64(size)

	// Read the actual data from the file at the specified position plus the length field width.
	data := make([]byte, dataLength)
	if _, err := s.File.ReadAt(data, int64(pos+lenWidth)); err != nil {
		return nil, err // Return nil slice and the encountered error if reading the data fails.
	}

	// Return the read data slice and no error.
	return data, nil
}

// ReadAt reads len(p) bytes from the file at the given offset 'off'.
// It acquires a lock to ensure exclusive access to shared resources.
// It flushes the buffer to ensure all buffered data is written to the underlying file.
// It returns the number of bytes read and any encountered error.
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	// Acquire a lock to ensure exclusive access to shared resources.
	s.mu.Lock()
	defer s.mu.Unlock() // Ensure unlocking the mutex when the function exits.

	// Flush the buffer to ensure all buffered data is written to the underlying file.
	if err := s.buf.Flush(); err != nil {
		return 0, err // Return 0 bytes read and the encountered error if flushing the buffer fails.
	}

	// Read len(p) bytes from the file at the given offset 'off' into the byte slice 'p'.
	// The number of bytes read is returned along with any encountered error.
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
