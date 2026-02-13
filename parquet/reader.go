package parquet

type ParquetReader struct{}

func NewReader() {

}

func (*ParquetReader) ReadRowGroup(i uint32) []byte { return nil }

func (*ParquetReader) Close() error { return nil }
