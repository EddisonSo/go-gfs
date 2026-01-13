package clientcli

import "fmt"

func printHelp() {
	fmt.Println(`Commands:
  ls [--namespace <name>] [prefix]            List files
  read [--namespace <name>] <path>            Read file to stdout
  read [--namespace <name>] <path> > <file>   Read file to local file
  write [--namespace <name>] <path> <data>   Write data to file
  write [--namespace <name>] <path> < <file> Write local file to GFS
  mv [--namespace <name>] <src> <dst>         Rename/move a file
  rm [--namespace <name>] <path>              Delete a file (use * to delete all in namespace)
  info [--namespace <name>] <path>            Show file information
  pressure                Show cluster resource pressure (CPU, memory, disk)
  help                    Show this help
  exit                    Quit the client`)
}
