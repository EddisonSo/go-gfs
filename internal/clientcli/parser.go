package clientcli

import (
	"fmt"
	"strings"
)

func parseArgs(line string) []string {
	var args []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for _, r := range line {
		if inQuote {
			if r == quoteChar {
				inQuote = false
			} else {
				current.WriteRune(r)
			}
		} else {
			if r == '"' || r == '\'' {
				inQuote = true
				quoteChar = r
			} else if r == ' ' || r == '\t' {
				if current.Len() > 0 {
					args = append(args, current.String())
					current.Reset()
				}
			} else {
				current.WriteRune(r)
			}
		}
	}
	if current.Len() > 0 {
		args = append(args, current.String())
	}
	return args
}

func extractNamespace(args []string) (string, []string, error) {
	var namespace string
	remaining := make([]string, 0, len(args))

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--namespace" || arg == "-n" {
			if i+1 >= len(args) {
				return "", nil, fmt.Errorf("missing namespace value")
			}
			namespace = args[i+1]
			i++
			continue
		}
		if strings.HasPrefix(arg, "--namespace=") {
			namespace = strings.TrimPrefix(arg, "--namespace=")
			continue
		}
		if strings.HasPrefix(arg, "-n=") {
			namespace = strings.TrimPrefix(arg, "-n=")
			continue
		}
		remaining = append(remaining, arg)
	}

	return namespace, remaining, nil
}
