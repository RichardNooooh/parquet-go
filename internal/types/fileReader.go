package types

import (
	"io"
)

type FileReader struct {
	Reader io.ReaderAt
	Size   int64
}

func NewReader(reader io.ReaderAt, size int64) *FileReader {
	return &FileReader{reader, size}
}
