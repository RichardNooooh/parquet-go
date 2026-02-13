package parquet

type ParquetWriter struct{}

func NewWriter() {

}

func (*ParquetWriter) Write(record string) error { return nil }

func (*ParquetWriter) Close() error { return nil }
