package metadata

import (
	"bytes"
	"testing"

	"github.com/RichardNooooh/parquet-go/internal/types"
)

func TestMagicNumbers(t *testing.T) {
	testcases := map[string]struct {
		data  []byte
		valid bool
	}{
		"valid0":            {data: []byte("PAR1\x00\x00\x00\x00PAR1"), valid: true},
		"valid1":            {data: []byte("PAR1\x04\x02\x02\x05PAR1"), valid: true},
		"valid2":            {data: []byte("PAR1\x00\x00\x00\x00\xF6PAR1"), valid: true},
		"valid3":            {data: []byte("PAR1\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00PAR1"), valid: true},
		"valid4":            {data: []byte("PAR1\x00\x00\x00\x00\x07\x00\x40PAR1"), valid: true},
		"valid5":            {data: []byte("PAR1\x00\x01\x50\x69\x00\x00\x00\x00PAR1"), valid: true},
		"invalidEmpty":      {data: []byte(""), valid: false},
		"invalidNearEmpty0": {data: []byte("P"), valid: false},
		"invalidNearEmpty1": {data: []byte("PAR"), valid: false},
		"invalidNearEmpty2": {data: []byte("PAR1"), valid: false},
		"invalidNearEmpty3": {data: []byte("PAR1\n"), valid: false},
		"invalid0":          {data: []byte("PAR11RAP"), valid: false},
		"invalid1":          {data: []byte("1RAPPAR1"), valid: false},
		"invalid2":          {data: []byte("PAR1\x00PAR1\n"), valid: false},
		"invalid3":          {data: []byte("1RAP1RAP"), valid: false},
		"invalid4":          {data: []byte("\nPAR1PAR1"), valid: false},
		"invalid5":          {data: []byte("par1\x00\x01\x50\x68par1"), valid: false},
		"invalid6":          {data: []byte("par2\x00\x01\x50\x68par2"), valid: false},
		"invalidSmall0":     {data: []byte("PAR1PAR1"), valid: false},
		"invalidSmall1":     {data: []byte("PAR1\x00PAR1"), valid: false},
		"invalidSmall2":     {data: []byte("PAR1\x00\x00PAR1"), valid: false},
		"invalidSmall3":     {data: []byte("PAR1\x00\x00\x00PAR1"), valid: false},
	}

	for name, test := range testcases {
		t.Run(name, func(t *testing.T) {
			reader := types.NewReader(bytes.NewReader(test.data), int64(len(test.data)))
			err := checkParquet(reader)

			if test.valid && err != nil {
				t.Errorf("expected valid result, got error: %v", err)
			}
		})
	}
}
