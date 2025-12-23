package metadata

// "file" refers to the file metadata located at the footer of the Parquet file
import (
	// "github.com/apache/thrift/lib/go/thrift"
	// "internal/gen-go/parquet"
	"bytes"
	"errors"
	"fmt"
	"os"
)

var PARQUET_MAGIC = []byte("PAR1")
var ErrorNotParquet = errors.New("not a Parquet file")

const WORD_LENGTH = 4

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
	return nil
}
