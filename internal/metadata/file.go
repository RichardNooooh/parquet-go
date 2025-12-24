package metadata

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/RichardNooooh/parquet-go/internal/types"

	format "github.com/RichardNooooh/parquet-go/internal/metadata/gen-go/parquet"
	thrift "github.com/apache/thrift/lib/go/thrift"
)

var parquetMagic = []byte("PAR1")
var ErrNotParquet = errors.New("not a Parquet file")

const wordLength = 4

func GetFileMetadata(ctx context.Context, file *types.FileReader) (*format.FileMetaData, error) {
	if err := checkParquet(file); err != nil {
		return nil, err
	}
	fileMetadataSize, err := getFileMetadataSize(file)
	if err != nil {
		return nil, err
	}

	compactMetadataBuffer := make([]byte, fileMetadataSize)
	count, err := file.Reader.ReadAt(compactMetadataBuffer, file.Size-2*wordLength-int64(fileMetadataSize))
	if err != nil {
		return nil, fmt.Errorf("unable to read footer metadata: %w", err)
	}
	if int64(count) < fileMetadataSize {
		return nil, fmt.Errorf("unable to read all footer metadata")
	}

	config := &thrift.TConfiguration{}
	thriftBuffer := thrift.NewTMemoryBufferLen(int(fileMetadataSize))
	_, err = thriftBuffer.Write(compactMetadataBuffer)
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

func GetPageLocations(fileMetadata *format.FileMetaData) ([]int64, error) {
	return nil, nil
}

func getFileMetadataSize(file *types.FileReader) (int64, error) {
	var fileMetadataLenBuffer [wordLength]byte

	count, err := file.Reader.ReadAt(fileMetadataLenBuffer[:], int64(file.Size-(2*wordLength)))
	if err != nil {
		return 0, fmt.Errorf("%w: missing file metadata size", ErrNotParquet)
	}
	if int64(count) < wordLength {
		return 0, fmt.Errorf("%w: could not read enough bytes for file metadata size", ErrNotParquet)
	}

	fileMetadataSize := int64(binary.LittleEndian.Uint32(fileMetadataLenBuffer[:]))
	if fileMetadataSize > file.Size-3*wordLength {
		return 0, fmt.Errorf("%w: file metadata too large (%d bytes)", ErrNotParquet, fileMetadataSize)
	} else if fileMetadataSize == 0 {
		return 0, fmt.Errorf("%w: file metadata is of size 0", ErrNotParquet)
	}

	return fileMetadataSize, nil
}
