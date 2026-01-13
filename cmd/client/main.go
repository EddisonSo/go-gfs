package main

import (
	"fmt"
	"os"

	"eddisonso.com/go-gfs/internal/clientcli"
)

func main() {
	if err := clientcli.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
