package metadata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/RichardNooooh/parquet-go/internal/types"
)

func TestTimeStoredFileMetadataSize(t *testing.T) {
	testcaseDir := filepath.Join(getTestcaseDirectory(), "timestored_examples")
	groundTruthFile := filepath.Join(testcaseDir, "fileMetadataSizes.json")
	testcases := *getExpectedFileMetadataSizes(groundTruthFile)
	for name, expectedSize := range testcases {
		t.Run(name, func(t *testing.T) {
			testfilename := filepath.Join(testcaseDir, name+".parquet")

			file, err := os.Open(testfilename)
			if err != nil {
				t.Errorf("%v: unable to open file %v", err, testfilename)
			}
			fileStat, err := file.Stat()
			if err != nil {
				t.Errorf("%v: unable to get filestat of %v", err, testfilename)
			}

			reader := types.NewReader(file, fileStat.Size())
			size, err := getFileMetadataSize(reader)
			if err != nil {
				t.Errorf("%v: failed to read filemetadatasize of %v", err, testfilename)
			}

			if size != expectedSize {
				t.Fatalf("%v: expected %d, got %d", testfilename, expectedSize, size)
			}
		})
	}
}

func getTestcaseDirectory() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		panic("runtime.Caller failed")
	}

	dir := filepath.Dir(thisFile)
	return filepath.Clean(filepath.Join(dir, "..", "..", "testdata"))
}

func getExpectedFileMetadataSizes(filename string) *map[string]int64 {
	file, err := os.ReadFile(filename)
	fmt.Println(filename)
	if err != nil {
		panic(err)
	}

	var sizes map[string]int64
	if err := json.Unmarshal(file, &sizes); err != nil {
		panic(err)
	}

	return &sizes
}

func TestFakeFileMetadataSize(t *testing.T) {
	testcases := map[string]struct {
		data  []byte
		valid bool
		size  int64
	}{
		"valid0":         {data: generateValidFakeParquet(1, "\x01\x00\x00\x00"), valid: true, size: 1},
		"valid1":         {data: generateValidFakeParquet(1024, "\x01\x00\x00\x00"), valid: true, size: 1},
		"validLarge0":    {data: generateValidFakeParquet(65536, "\x00\x00\x01\x00"), valid: true, size: 65536},
		"validLarge1":    {data: generateValidFakeParquet(130000, "\xDE\xFA\x01\x00"), valid: true, size: 129758},
		"validLarge2":    {data: generateValidFakeParquet(1082802, "\xB2\x85\x10\x00"), valid: true, size: 1082802},
		"validVeryLarge": {data: generateValidFakeParquet(294070845, "\x3D\x2A\x87\x01"), valid: true, size: 25635389},
		"invalidEmpty":   {data: generateValidFakeParquet(16, "\x00\x00\x00\x00"), valid: false, size: 0},
		"invalid0":       {data: generateValidFakeParquet(1, "\xFF\xFF\xFF\xFF"), valid: false, size: 0},
		"invalid1":       {data: generateValidFakeParquet(1, "\x02\x00\x00\x00"), valid: false, size: 0},
	}

	for name, test := range testcases {
		t.Run(name, func(t *testing.T) {
			reader := types.NewReader(bytes.NewReader(test.data), int64(len(test.data)))
			size, err := getFileMetadataSize(reader)

			if test.valid && err != nil {
				t.Errorf("expected valid result, got error: %v", err)
			} else if !test.valid && err == nil {
				t.Errorf("expected invalid result, did not get error")
			} else if test.valid && size != test.size {
				t.Errorf("expected size of %d bytes, got %v bytes", test.size, size)
			}
		})
	}
}

func generateValidFakeParquet(size uint32, littleEndianSize string) []byte {
	fileLength := size + 12 // 2*4 (magic length) + 4 (filemetadata uint32 size)
	buffer := make([]byte, fileLength)

	// magic values
	copy(buffer[:4], []byte("PAR1"))
	copy(buffer[fileLength-4:], []byte("PAR1"))

	// FileMetaData size field
	copy(buffer[fileLength-8:fileLength-4], []byte(littleEndianSize))

	return buffer
}
