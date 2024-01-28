package server

import (
	"fmt"
	"sync"
)

var (
	ErrOffsetNotFound = fmt.Errorf("offset not found")
)

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

type Log struct {
	records []Record
	mu      sync.Mutex
}

func NewLog() *Log {
	return &Log{}
}

func (l *Log) Append(record Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	size := len(l.records)
	record.Offset = uint64(size)
	l.records = append(l.records, record)
	return uint64(size), nil
}

func (l *Log) Read(offset uint64) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if offset >= uint64(len(l.records)) {
		return Record{}, ErrOffsetNotFound
	}

	return l.records[offset], nil
}
