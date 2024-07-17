package wal

const (
	maxSizeSegment uint32 = 20971520
	dirPath        string = "./db/data"
)

type options struct {
	maxSizeSegment uint32
	dirPath        string
}

type Option func(options *options)

func WithMaxSizeSegment(size uint32) Option {
	return func(o *options) {
		o.maxSizeSegment = size
	}
}

func WithDirPath(dirPath string) Option {
	return func(o *options) {
		o.dirPath = dirPath
	}
}
