package metadata

// "file" refers to the file metadata located at the footer of the Parquet file
import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"

	genparquet "github.com/RichardNooooh/parquet-go/internal/metadata/internal/gen-go/parquet"
	thrift "github.com/apache/thrift/lib/go/thrift"
)

var PARQUET_MAGIC = []byte("PAR1")
var ErrorNotParquet = errors.New("not a Parquet file")

const WORD_LENGTH = 4

func ProcessFooterMetadata(file *os.File) error {
	footerMetadataSize, err := getFooterMetadataSize(file)
	if err != nil {
		return err
	}

	fileStat, err := file.Stat()
	if err != nil {
		return err
	}

	compactMetadataBuffer := make([]byte, footerMetadataSize)
	count, err := file.ReadAt(compactMetadataBuffer, fileStat.Size()-2*WORD_LENGTH-int64(footerMetadataSize))
	if err != nil {
		return fmt.Errorf("unable to read footer metadata: %w", err)
	}
	if count < int(footerMetadataSize) {
		return fmt.Errorf("unable to read all footer metadata")
	}
	log.Printf("Retrieved footer metadata\n")

	config := &thrift.TConfiguration{}
	thriftBuffer := thrift.NewTMemoryBufferLen(int(footerMetadataSize))
	_, err = thriftBuffer.Write(compactMetadataBuffer)
	if err != nil {
		return fmt.Errorf("failed to transfer footer metadata to thrift buffer: %w", err)
	}

	protocolConfig := thrift.NewTCompactProtocolConf(thriftBuffer, config)
	metadata := genparquet.NewFileMetaData()
	err = metadata.Read(context.Background(), protocolConfig)
	if err != nil {
		return fmt.Errorf("failed to decode thrift metadata: %w", err)
	}

	log.Printf("Processed Thrift Metadata Structure: %v\n", metadata)

	return nil
}

func getFooterMetadataSize(file *os.File) (uint32, error) {
	var footerLengthBuffer [WORD_LENGTH]byte

	fileStat, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat file error: %w", err)
	}

	fileSize := fileStat.Size()
	count, err := file.ReadAt(footerLengthBuffer[:], fileSize-(2*WORD_LENGTH))
	if err != nil {
		return 0, fmt.Errorf("%w: missing file metadata size", ErrorNotParquet)
	}
	if count < WORD_LENGTH {
		return 0, fmt.Errorf("%w: could not read enough bytes for file metadata size", ErrorNotParquet)
	}

	footerMetadataSize := binary.LittleEndian.Uint32(footerLengthBuffer[:])
	log.Printf("file metadata size: %d\n", footerMetadataSize)

	return footerMetadataSize, nil
}

func checkParquet(file *os.File) error {
	fileStat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat file error: %w", err)
	}

	size := fileStat.Size()
	if size < 3*WORD_LENGTH {
		return fmt.Errorf("%w: file is too small! minimum size: %d, actual size: %d", ErrorNotParquet, 3*WORD_LENGTH, size)
	}

	var buffer [WORD_LENGTH]byte

	_, err = file.ReadAt(buffer[:], 0)
	if err != nil {
		return fmt.Errorf("could not read enough bytes at start of file: %w", err)
	}

	// check first 4
	hasHeaderMagic := bytes.Equal(buffer[:], PARQUET_MAGIC)
	if !hasHeaderMagic {
		return fmt.Errorf("%w: header magic mismatch: got %q", ErrorNotParquet, buffer[:])
	}

	_, err = file.ReadAt(buffer[:], size-int64(WORD_LENGTH))
	if err != nil {
		return fmt.Errorf("could not read enough bytes at end of file: %w", err)
	}

	// check last 4
	hasFooterMagic := bytes.Equal(buffer[:], PARQUET_MAGIC)
	if !hasFooterMagic {
		return fmt.Errorf("%w: footer magic mismatch: got %q", ErrorNotParquet, buffer[:])
	}

	return nil
}

func Start(fileName string) error {
	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("no such file exists: %w", err)
	}
	defer file.Close()

	err = checkParquet(file)
	if err != nil {
		return fmt.Errorf("%v had Parquet check errors: %w", fileName, err)
	}
	err = ProcessFooterMetadata(file)
	if err != nil {
		return err
	}
	return nil
}
