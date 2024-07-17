package wal

import (
	"io"
	"os"
)

const readByteSize = 64

type Segment struct {
	id             uint
	fd             *os.File
	size           uint32
	maxSizeSegment uint32
}

func (s *Segment) Write(data []byte) error {
	n, err := s.fd.Write(data)
	if err != nil {
		return err
	}
	s.size += uint32(n)

	return nil
}

func (s *Segment) Sync() error {
	return s.fd.Sync()
}

func (s *Segment) Read() ([]byte, error) {
	var logs []byte

	data := make([]byte, readByteSize)

	for {
		n, err := s.fd.Read(data)
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		logs = append(logs, data[:n]...)
	}

	return logs, nil
}

func (s *Segment) Close() error {
	return s.fd.Close()
}

func (s *Segment) isFull(buf int) bool {
	return (s.size + uint32(buf)) > s.maxSizeSegment
}

func (s *Segment) ID() uint {
	return s.id
}

func (s *Segment) FileName() string {
	return s.fd.Name()
}
