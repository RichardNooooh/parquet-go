package parquet

import ()

// These are types that should either be provided in types/ or in their respective modules
type ParquetReader struct{}
type ParquetWriter struct{}

func NewReader() {

}

func NewWriter() {

}

func (*ParquetReader) ReadRowGroup(i uint32) []byte { return nil }

func (*ParquetReader) Close() error { return nil }

func (*ParquetWriter) Write(record string) error { return nil }

func (*ParquetWriter) Close() error { return nil }
