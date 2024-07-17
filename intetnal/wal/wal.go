package wal

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"jokedb/intetnal/storage/engine"
	"os"
	"path/filepath"
	"sort"
)

const segmentFileExt = ".seg"

type WAL struct {
	opts          options
	activeSegment *Segment
	oldSegmentIDs []uint
}

type LogData struct {
	Action engine.ActionType
	Key,
	Value string
}

func (w *WAL) Write(logs []LogData) error {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(logs)
	if err != nil {
		return err
	}

	if w.activeSegment.isFull(buf.Len()) {
		err = w.NewActiveSegment()
		if err != nil {
			return err
		}
	}

	err = w.activeSegment.Write(buf.Bytes())
	if err != nil {
		return err
	}

	return w.activeSegment.Sync()
}

func (w *WAL) ActiveSegment() *Segment {
	return w.activeSegment
}

func (w *WAL) ReadSegments() ([]LogData, error) {
	segmentIDs := make([]uint, 0, len(w.oldSegmentIDs)+1)
	segmentIDs = append(segmentIDs, w.activeSegment.id)
	segmentIDs = append(segmentIDs, w.oldSegmentIDs...)

	var logs []LogData
	var segs []*Segment
	defer func() {
		for _, seg := range segs {
			_ = seg.Close()
		}
	}()

	for _, id := range segmentIDs {
		seg, err := openSegmentFile(w.opts.dirPath, id, w.opts.maxSizeSegment)
		if err != nil {
			return nil, err
		}
		segs = append(segs, seg)
		data, err := seg.Read()
		if err != nil {
			return nil, err
		}

		buf := bytes.NewBuffer(data)
		for {
			var batch []LogData
			decoder := gob.NewDecoder(buf)
			err = decoder.Decode(&batch)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, err
			}

			logs = append(logs, batch...)
		}
	}

	return logs, nil
}

func (w *WAL) Close() error {
	return w.activeSegment.fd.Close()
}

func (w *WAL) NewActiveSegment() error {
	newID := w.activeSegment.id + 1
	seg, err := openSegmentFile(w.opts.dirPath, newID, w.opts.maxSizeSegment)
	if err != nil {
		return err
	}

	w.oldSegmentIDs = append(w.oldSegmentIDs, w.activeSegment.id)
	w.activeSegment = seg

	return nil
}

func Open(opts ...Option) (*WAL, error) {
	var defaultOpts = options{
		maxSizeSegment: maxSizeSegment,
		dirPath:        dirPath,
	}

	o := defaultOpts
	for _, opt := range opts {
		opt(&o)
	}

	wal := &WAL{
		opts: o,
	}

	if err := os.MkdirAll(wal.opts.dirPath, os.ModePerm); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(wal.opts.dirPath)
	if err != nil {
		return nil, err
	}

	var segmentIDs []uint
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id uint
		_, err = fmt.Sscanf(entry.Name(), "%d"+segmentFileExt, &id)
		if err != nil {
			continue
		}
		segmentIDs = append(segmentIDs, id)
	}

	var activeSegment *Segment
	if len(segmentIDs) == 0 {
		activeSegment, err = openSegmentFile(wal.opts.dirPath, 1, wal.opts.maxSizeSegment)
		if err != nil {
			return nil, err
		}
	} else {
		sort.Slice(segmentIDs, func(i, j int) bool { return segmentIDs[i] < segmentIDs[j] })

		activeSegment, err = openSegmentFile(wal.opts.dirPath, segmentIDs[len(segmentIDs)-1], wal.opts.maxSizeSegment)
		if err != nil {
			return nil, err
		}

		wal.oldSegmentIDs = segmentIDs[:len(segmentIDs)-1]
	}

	wal.activeSegment = activeSegment

	return wal, nil
}

func openSegmentFile(dirPath string, id uint, maxSizeSegment uint32) (*Segment, error) {
	fd, err := os.OpenFile(
		SegmentFileName(dirPath, id),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)

	if err != nil {
		return nil, err
	}

	return &Segment{
		fd:             fd,
		id:             id,
		maxSizeSegment: maxSizeSegment,
	}, nil
}

func SegmentFileName(dirPath string, id uint) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+segmentFileExt, id))
}
