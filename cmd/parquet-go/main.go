package main

import (
	"fmt"
	"github.com/RichardNooooh/parquet-go/internal/metadata"
	"os"
)

func main() {

	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: parquet-go <file>")
		os.Exit(2)
	}

	err := metadata.Start(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Println("Ok")
}
