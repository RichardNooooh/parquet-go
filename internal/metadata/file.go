package metadata

// "file" refers to the file metadata located at the footer of the Parquet file
import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"

	genparquet "github.com/RichardNooooh/parquet-go/internal/metadata/gen-go/parquet"
	thrift "github.com/apache/thrift/lib/go/thrift"
)

var PARQUET_MAGIC = []byte("PAR1")
var ErrNotParquet = errors.New("not a Parquet file")

const WORD_LENGTH int64 = 4

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
	count, err := file.reader.ReadAt(compactMetadataBuffer, file.size-2*WORD_LENGTH-int64(fileMetadataSize))
	if err != nil {
		return nil, fmt.Errorf("unable to read footer metadata: %w", err)
	}
	if count < int(fileMetadataSize) {
		return nil, fmt.Errorf("unable to read all footer metadata")
	}
	log.Printf("Retrieved footer metadata\n")

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

func getFileMetadataSize(file *FileReader) (uint32, error) {
	var fileMetadataLengthBuffer [WORD_LENGTH]byte

	count, err := file.reader.ReadAt(fileMetadataLengthBuffer[:], int64(file.size-(2*WORD_LENGTH)))
	if err != nil {
		return 0, fmt.Errorf("%w: missing file metadata size", ErrNotParquet)
	}
	if int64(count) < WORD_LENGTH {
		return 0, fmt.Errorf("%w: could not read enough bytes for file metadata size", ErrNotParquet)
	}

	fileMetadataSize := binary.LittleEndian.Uint32(fileMetadataLengthBuffer[:])
	if int64(fileMetadataSize) > file.size-2*WORD_LENGTH {
		return 0, fmt.Errorf("%w: file metadata too large (%d bytes)", ErrNotParquet, fileMetadataSize)
	} else if fileMetadataSize == 0 {
		return 0, fmt.Errorf("%w: file metadata is of size 0", ErrNotParquet)
	}

	log.Printf("file metadata size: %d\n", fileMetadataSize)

	return fileMetadataSize, nil
}

func checkParquet(file *FileReader) error {
	size := file.size
	if size < 3*WORD_LENGTH {
		return fmt.Errorf("%w: file is too small! minimum size: %d, actual size: %d", ErrNotParquet, 3*WORD_LENGTH, size)
	}

	var buffer [WORD_LENGTH]byte

	_, err := file.reader.ReadAt(buffer[:], 0)
	if err != nil {
		return fmt.Errorf("could not read enough bytes at start of file: %w", err)
	}

	// check first 4
	hasHeaderMagic := bytes.Equal(buffer[:], PARQUET_MAGIC)
	if !hasHeaderMagic {
		return fmt.Errorf("%w: header magic mismatch: got %q", ErrNotParquet, buffer[:])
	}

	_, err = file.reader.ReadAt(buffer[:], int64(size-WORD_LENGTH))
	if err != nil {
		return fmt.Errorf("could not read enough bytes at end of file: %w", err)
	}

	// check last 4
	hasFooterMagic := bytes.Equal(buffer[:], PARQUET_MAGIC)
	if !hasFooterMagic {
		return fmt.Errorf("%w: footer magic mismatch: got %q", ErrNotParquet, buffer[:])
	}

	return nil
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
