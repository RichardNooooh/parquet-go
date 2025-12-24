package metadata

import (
	"bytes"
	"fmt"

	"github.com/RichardNooooh/parquet-go/internal/types"
)

func checkParquet(file *types.FileReader) error {
	size := file.Size
	if size < 3*wordLength {
		return fmt.Errorf("%w: file is too small! minimum size: %d, actual size: %d", ErrNotParquet, 3*wordLength, size)
	}

	var buffer [wordLength]byte

	_, err := file.Reader.ReadAt(buffer[:], 0)
	if err != nil {
		return fmt.Errorf("could not read enough bytes at start of file: %w", err)
	}

	// check first 4
	hasHeaderMagic := bytes.Equal(buffer[:], parquetMagic)
	if !hasHeaderMagic {
		return fmt.Errorf("%w: header magic mismatch: got %q", ErrNotParquet, buffer[:])
	}

	_, err = file.Reader.ReadAt(buffer[:], int64(size-wordLength))
	if err != nil {
		return fmt.Errorf("could not read enough bytes at end of file: %w", err)
	}

	// check last 4
	hasFooterMagic := bytes.Equal(buffer[:], parquetMagic)
	if !hasFooterMagic {
		return fmt.Errorf("%w: footer magic mismatch: got %q", ErrNotParquet, buffer[:])
	}

	return nil
}
