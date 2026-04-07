package parquet

import (
	"github.com/RichardNooooh/parquet-go/metadata"
	"github.com/RichardNooooh/parquet-go/schema"
	"io"
)

type ParquetReader struct{}

func NewReader() {

}

func Open(r io.ReaderAt, size int64, opts ...ParquetReaderOption) (*ParquetReader, error) {
	return nil, nil
}

func (r *ParquetReader) GetMeta() *metadata.FileMeta { return nil }

func (r *ParquetReader) GetSchema() *schema.SchemaElement { return nil }

// func (*ParquetReader) ReadRowGroup(i uint32) []byte { return nil }

func (*ParquetReader) Close() error { return nil }
