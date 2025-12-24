package main

import (
	"context"
	"fmt"
	"os"

	"github.com/RichardNooooh/parquet-go/internal/metadata"
	// "github.com/RichardNooooh/parquet-go/internal/metadata/gen-go/parquet"
	"github.com/RichardNooooh/parquet-go/internal/types"
)

func run() error {
	file, err := os.Open(os.Args[1])
	if err != nil {
		return err
	}
	defer file.Close()
	fileStat, err := file.Stat()
	if err != nil {
		return err
	}

	fileReader := types.NewReader(file, fileStat.Size())
	fileMetadata, err := metadata.GetFileMetadata(context.Background(), fileReader)
	if err != nil {
		return err
	}

	var output any

	switch command := os.Args[2]; command {
	case "fileVersion":
		output = fileMetadata.GetVersion()
	case "schema":
		output = fileMetadata.GetSchema()
	case "numRows":
		output = fileMetadata.GetNumRows()
	case "rowGroups":
		output = fileMetadata.GetRowGroups()
	case "keyValues":
		output = fileMetadata.GetKeyValueMetadata()
	case "createdBy":
		output = fileMetadata.GetCreatedBy()
	case "columnOrders":
		output = fileMetadata.GetColumnOrders()
	case "encryptionAlgo":
		output = fileMetadata.GetEncryptionAlgorithm()
	case "signingKeyMetadata":
		output = fileMetadata.GetFooterSigningKeyMetadata()
	}

	fmt.Printf("%v", output)
	// if os.Args[2] == "fileVersion" {
	// 	output = fileMetadata.GetVersion()
	// } else if os.Args[2] == "schema" {
	// 	output = fileMetadata.GetSchema()
	// } else if os.Args[2] == "numrows" {
	// 	output = fileMetadata.GetNumRows()
	// } else if

	return nil
}

func main() {

	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: parquet-go <file> <cmd>")
		os.Exit(2)
	}

	err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Println("Ok")
}
