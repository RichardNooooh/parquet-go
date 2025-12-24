package metadata

// "file" refers to the file metadata located at the footer of the Parquet file
import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	genparquet "github.com/RichardNooooh/parquet-go/internal/metadata/gen-go/parquet"
	thrift "github.com/apache/thrift/lib/go/thrift"
)

var parquetMagic = []byte("PAR1")
var ErrNotParquet = errors.New("not a Parquet file")

const wordLength = 4

type FileReader struct {
	reader io.ReaderAt
	size   int64
}

func GetFileMetadata(ctx context.Context, file *FileReader) (*genparquet.FileMetaData, error) {
	if err := checkParquet(file); err != nil {
		return nil, err
	}
	fileMetadataSize, err := getFileMetadataSize(file)
	if err != nil {
		return nil, err
	}

	compactMetadataBuffer := make([]byte, fileMetadataSize)
	count, err := file.reader.ReadAt(compactMetadataBuffer, file.size-2*wordLength-int64(fileMetadataSize))
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
	fileMetadata := genparquet.NewFileMetaData()
	err = fileMetadata.Read(ctx, protocolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to decode thrift metadata: %w", err)
	}

	return fileMetadata, nil
}

func GetPageLocations(fileMetadata *genparquet.FileMetaData) ([]int64, error) {
	return nil, nil
}

func getFileMetadataSize(file *FileReader) (int64, error) {
	var fileMetadataLengthBuffer [wordLength]byte

	count, err := file.reader.ReadAt(fileMetadataLengthBuffer[:], int64(file.size-(2*wordLength)))
	if err != nil {
		return 0, fmt.Errorf("%w: missing file metadata size", ErrNotParquet)
	}
	if int64(count) < wordLength {
		return 0, fmt.Errorf("%w: could not read enough bytes for file metadata size", ErrNotParquet)
	}

	fileMetadataSize := int64(binary.LittleEndian.Uint32(fileMetadataLengthBuffer[:]))
	if fileMetadataSize > file.size-2*wordLength {
		return 0, fmt.Errorf("%w: file metadata too large (%d bytes)", ErrNotParquet, fileMetadataSize)
	} else if fileMetadataSize == 0 {
		return 0, fmt.Errorf("%w: file metadata is of size 0", ErrNotParquet)
	}

	return fileMetadataSize, nil
}

// func Start(fileName string) error {
// 	file, err := os.Open(fileName)
// 	if err != nil {
// 		return fmt.Errorf("no such file exists: %w", err)
// 	}
// 	defer file.Close()
//
// 	fileStat, err := file.Stat()
// 	if err != nil {
// 		return fmt.Errorf("could not read file size: %w", err)
// 	}
//
// 	fileReader := &FileReader{file, fileStat.Size()}
// 	err = checkParquet(fileReader)
// 	if err != nil {
// 		return fmt.Errorf("%v had Parquet check errors: %w", fileName, err)
// 	}
// 	_, err = ProcessFileMetadata(fileReader)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }
