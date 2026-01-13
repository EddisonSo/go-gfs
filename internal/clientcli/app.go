package clientcli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/chzyer/readline"

	gfs "eddisonso.com/go-gfs/pkg/go-gfs-sdk"
)

var commands = []string{"ls", "cat", "read", "write", "rm", "mv", "rename", "info", "pressure", "help", "exit", "quit"}

type App struct {
	masterAddr string
	client     *gfs.Client
}

func Run(args []string) error {
	app := &App{
		masterAddr: "localhost:9000",
	}
	app.parseFlags(args)
	if err := app.connect(); err != nil {
		return err
	}
	defer app.client.Close()

	fmt.Printf("Connected to %s\n", app.masterAddr)
	fmt.Println("Type 'help' for commands, 'exit' to quit")
	fmt.Println()

	completer := readline.NewPrefixCompleter(
		readline.PcItem("ls", readline.PcItemDynamic(app.completeGFSPath)),
		readline.PcItem("cat", readline.PcItemDynamic(app.completeGFSPath)),
		readline.PcItem("read", readline.PcItemDynamic(app.completeGFSPath)),
		readline.PcItem("write", readline.PcItemDynamic(app.completeGFSPath)),
		readline.PcItem("rm", readline.PcItemDynamic(app.completeGFSPath)),
		readline.PcItem("rmns"),
		readline.PcItem("mv", readline.PcItemDynamic(app.completeGFSPath)),
		readline.PcItem("rename", readline.PcItemDynamic(app.completeGFSPath)),
		readline.PcItem("info", readline.PcItemDynamic(app.completeGFSPath)),
		readline.PcItem("pressure"),
		readline.PcItem("help"),
		readline.PcItem("exit"),
		readline.PcItem("quit"),
	)

	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "gfs> ",
		AutoComplete:    completer,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		return fmt.Errorf("failed to initialize readline: %w", err)
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			continue
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		args := parseArgs(line)
		if len(args) == 0 {
			continue
		}

		if err := app.dispatch(args[0], args[1:]); err != nil {
			fmt.Printf("Error: %v\n", err)
		}
	}

	return nil
}

func (a *App) completeGFSPath(line string) []string {
	// Extract the path prefix being typed
	parts := strings.Fields(line)
	prefix := ""
	if len(parts) > 0 {
		prefix = parts[len(parts)-1]
	}

	// Fetch files from GFS
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	files, err := a.client.ListFiles(ctx, prefix)
	if err != nil {
		return nil
	}

	var completions []string
	for _, f := range files {
		completions = append(completions, f.Path)
	}
	return completions
}

func (a *App) parseFlags(args []string) {
	for i := 0; i < len(args); i++ {
		if args[i] == "-master" && i+1 < len(args) {
			a.masterAddr = args[i+1]
		}
	}
}

func (a *App) connect() error {
	fmt.Printf("Connecting to master at %s...\n", a.masterAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	client, err := gfs.New(ctx, a.masterAddr)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to connect to master: %w", err)
	}
	a.client = client

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	_, err = a.client.ListFiles(ctx, "")
	cancel()
	if err != nil {
		return fmt.Errorf("failed to connect to master: %w", err)
	}

	return nil
}

func (a *App) dispatch(cmd string, args []string) error {
	switch cmd {
	case "ls":
		return a.cmdLs(args)
	case "read", "cat":
		return a.cmdRead(args)
	case "write":
		return a.cmdWrite(args)
	case "mv", "rename":
		return a.cmdMv(args)
	case "rm":
		return a.cmdRm(args)
	case "rmns":
		return a.cmdRmns(args)
	case "info":
		return a.cmdInfo(args)
	case "pressure":
		return a.cmdPressure(args)
	case "help":
		printHelp()
		return nil
	case "exit", "quit":
		fmt.Println("Goodbye!")
		os.Exit(0)
	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}
	return nil
}
