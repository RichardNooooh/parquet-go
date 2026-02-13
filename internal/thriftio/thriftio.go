package thriftio

import (
	"context"
	"fmt"
	format "github.com/RichardNooooh/parquet-go/internal/format/gen-go/parquet"
	thrift "github.com/apache/thrift/lib/go/thrift"
)

func DecodeFileMetadata(ctx context.Context, buffer []byte, size int64) (*format.FileMetaData, error) {
	config := &thrift.TConfiguration{}
	thriftBuffer := thrift.NewTMemoryBufferLen(int(size))
	_, err := thriftBuffer.Write(buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to transfer footer metadata to thrift buffer: %w", err)
	}

	protocolConfig := thrift.NewTCompactProtocolConf(thriftBuffer, config)
	fileMetadata := format.NewFileMetaData()
	err = fileMetadata.Read(ctx, protocolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to decode thrift metadata: %w", err)
	}

	return fileMetadata, nil
}
