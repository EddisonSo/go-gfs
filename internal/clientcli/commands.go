package clientcli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	gfs "eddisonso.com/go-gfs/pkg/go-gfs-sdk"
)

func getContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 120*time.Second)
}

func (a *App) cmdLs(args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	namespace, remaining, err := extractNamespace(args)
	if err != nil {
		return fmt.Errorf("usage error: %w", err)
	}

	prefix := ""
	if len(remaining) > 0 {
		prefix = remaining[0]
	}

	files, err := a.client.ListFilesWithNamespace(ctx, namespace, prefix)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		fmt.Println("No files found")
		return nil
	}

	renderFileTable(os.Stdout, files)
	return nil
}

func (a *App) cmdRead(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: read [--namespace <name>] <path> [> localfile]")
	}

	namespace, remaining, err := extractNamespace(args)
	if err != nil {
		return fmt.Errorf("usage error: %w", err)
	}
	if len(remaining) < 1 {
		return fmt.Errorf("usage: read [--namespace <name>] <path> [> localfile]")
	}

	path := remaining[0]
	var outputFile string

	for i, arg := range remaining {
		if arg == ">" && i+1 < len(remaining) {
			outputFile = remaining[i+1]
			break
		}
	}

	ctx, cancel := getContext()
	defer cancel()

	var totalSize int64
	if fileInfo, err := a.client.GetFileWithNamespace(ctx, path, namespace); err == nil {
		totalSize = int64(fileInfo.Size)
	}

	var output io.Writer
	var outFile *os.File
	if outputFile != "" {
		outFile, err = os.Create(outputFile)
		if err != nil {
			return fmt.Errorf("failed to create output file: %w", err)
		}
		defer outFile.Close()
		output = outFile
	} else {
		output = os.Stdout
	}

	var progress *TransferProgress
	if totalSize > 0 {
		progress = NewTransferProgress(totalSize, "Downloading")
		progress.Start()
		defer progress.Finish()
		output = &ProgressWriter{w: output, progress: progress}
	}

	n, err := a.client.ReadToWithNamespace(ctx, path, namespace, output)
	if err != nil {
		return err
	}

	if outputFile == "" {
		fmt.Println()
	} else if progress == nil {
		fmt.Printf("Wrote %d bytes to %s\n", n, outputFile)
	}

	return nil
}

func (a *App) cmdWrite(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: write [--namespace <name>] <path> <data>  OR  write [--namespace <name>] <path> < localfile")
	}

	namespace, remaining, err := extractNamespace(args)
	if err != nil {
		return fmt.Errorf("usage error: %w", err)
	}
	if len(remaining) < 1 {
		return fmt.Errorf("usage: write [--namespace <name>] <path> <data>  OR  write [--namespace <name>] <path> < localfile")
	}

	path := remaining[0]

	inputFile := ""
	for i, arg := range remaining {
		if arg == "<" && i+1 < len(remaining) {
			inputFile = remaining[i+1]
			break
		}
	}

	ctx, cancel := getContext()
	defer cancel()

	a.client.CreateFileWithNamespace(ctx, path, namespace)

	if inputFile != "" {
		totalWritten, err := writeFromFile(a.client, ctx, path, inputFile, namespace)
		if err != nil {
			return fmt.Errorf("write failed: %w", err)
		}
		fmt.Printf("Wrote %d bytes to %s\n", totalWritten, path)
		return nil
	}

	if len(remaining) > 1 && remaining[1] != "<" {
		writeData := []byte(strings.Join(remaining[1:], " "))
		if len(writeData) == 0 {
			return fmt.Errorf("no data to write")
		}
		n, err := a.client.WriteWithNamespace(ctx, path, namespace, writeData)
		if err != nil {
			return fmt.Errorf("write failed: %w", err)
		}
		fmt.Printf("Wrote %d bytes to %s\n", n, path)
		return nil
	}

	return fmt.Errorf("usage: write [--namespace <name>] <path> <data>  OR  write [--namespace <name>] <path> < localfile")
}

func writeFromFile(client *gfs.Client, ctx context.Context, gfsPath, localPath, namespace string) (int64, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}
	totalSize := stat.Size()

	prepared, err := client.PrepareUploadWithNamespace(ctx, gfsPath, namespace, totalSize)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare upload: %w", err)
	}

	var progress *TransferProgress
	var reader io.Reader = file
	if totalSize > 0 {
		progress = NewTransferProgress(totalSize, "Uploading")
		progress.Start()
		defer progress.Finish()
		reader = &ProgressReader{r: file, progress: progress}
	}

	return prepared.AppendFrom(ctx, reader)
}

func (a *App) cmdRm(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: rm [--namespace <name>] <path>")
	}
	namespace, remaining, err := extractNamespace(args)
	if err != nil {
		return fmt.Errorf("usage error: %w", err)
	}
	if len(remaining) < 1 {
		return fmt.Errorf("usage: rm [--namespace <name>] <path>")
	}
	path := remaining[0]

	ctx, cancel := getContext()
	defer cancel()

	if err := a.client.DeleteFileWithNamespace(ctx, path, namespace); err != nil {
		return err
	}

	fmt.Printf("Deleted %s\n", path)
	return nil
}

func (a *App) cmdMv(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: mv [--namespace <name>] <source> <destination>")
	}
	namespace, remaining, err := extractNamespace(args)
	if err != nil {
		return fmt.Errorf("usage error: %w", err)
	}
	if len(remaining) < 2 {
		return fmt.Errorf("usage: mv [--namespace <name>] <source> <destination>")
	}
	oldPath := remaining[0]
	newPath := remaining[1]

	ctx, cancel := getContext()
	defer cancel()

	if err := a.client.RenameFileWithNamespace(ctx, oldPath, newPath, namespace); err != nil {
		return err
	}

	fmt.Printf("Renamed %s -> %s\n", oldPath, newPath)
	return nil
}

func (a *App) cmdInfo(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: info [--namespace <name>] <path>")
	}
	namespace, remaining, err := extractNamespace(args)
	if err != nil {
		return fmt.Errorf("usage error: %w", err)
	}
	if len(remaining) < 1 {
		return fmt.Errorf("usage: info [--namespace <name>] <path>")
	}
	path := remaining[0]

	ctx, cancel := getContext()
	defer cancel()

	f, err := a.client.GetFileWithNamespace(ctx, path, namespace)
	if err != nil {
		return err
	}

	displayNamespace := f.Namespace
	if displayNamespace == "" {
		displayNamespace = gfs.DefaultNamespace
	}

	fmt.Printf("Path:       %s\n", f.Path)
	fmt.Printf("Namespace:  %s\n", displayNamespace)
	fmt.Printf("Size:       %d bytes\n", f.Size)
	fmt.Printf("Chunk Size: %d bytes\n", f.ChunkSize)
	fmt.Printf("Chunks:     %d\n", len(f.ChunkHandles))

	chunks, err := a.client.GetChunkLocationsWithNamespace(ctx, path, displayNamespace)
	if err == nil && len(chunks) > 0 {
		fmt.Println("\nChunk Details:")
		for i, chunk := range chunks {
			fmt.Printf("  [%d] %s (%d bytes)\n", i, chunk.ChunkHandle, chunk.Size)
			if chunk.Primary != nil {
				fmt.Printf("      Primary: %s:%d\n", chunk.Primary.Hostname, chunk.Primary.DataPort)
			}
			for _, loc := range chunk.Locations {
				if chunk.Primary == nil || loc.ServerId != chunk.Primary.ServerId {
					fmt.Printf("      Replica: %s:%d\n", loc.Hostname, loc.DataPort)
				}
			}
		}
	}

	return nil
}

func (a *App) cmdPressure(args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	resp, err := a.client.GetClusterPressure(ctx)
	if err != nil {
		return err
	}

	fmt.Println("Cluster Status")
	fmt.Println("==============")
	fmt.Println()

	if resp.MasterBuildInfo != nil {
		fmt.Printf("Master Build: %s (%s)\n", resp.MasterBuildInfo.BuildId, resp.MasterBuildInfo.BuildTime)
	}
	fmt.Println()

	if len(resp.Servers) == 0 {
		fmt.Println("No chunkservers registered")
		return nil
	}

	for _, server := range resp.Servers {
		status := "ALIVE"
		if !server.IsAlive {
			status = "DEAD"
		}

		fmt.Printf("Server: %s (%s:%d) [%s]\n",
			server.Server.ServerId,
			server.Server.Hostname,
			server.Server.DataPort,
			status)

		if server.BuildInfo != nil {
			fmt.Printf("  Build:  %s (%s)\n", server.BuildInfo.BuildId, server.BuildInfo.BuildTime)
		}
		fmt.Printf("  Chunks: %d\n", server.ChunkCount)

		if server.Resources != nil {
			r := server.Resources

			cpuBar := progressBar(r.CpuUsagePercent, 20)
			fmt.Printf("  CPU:    [%s] %5.1f%%\n", cpuBar, r.CpuUsagePercent)

			memBar := progressBar(r.MemoryUsagePercent, 20)
			memUsedGB := float64(r.MemoryUsedBytes) / (1024 * 1024 * 1024)
			memTotalGB := float64(r.MemoryTotalBytes) / (1024 * 1024 * 1024)
			fmt.Printf("  Memory: [%s] %5.1f%% (%.1f/%.1f GB)\n",
				memBar, r.MemoryUsagePercent, memUsedGB, memTotalGB)

			diskBar := progressBar(r.DiskUsagePercent, 20)
			diskUsedGB := float64(r.DiskUsedBytes) / (1024 * 1024 * 1024)
			diskTotalGB := float64(r.DiskTotalBytes) / (1024 * 1024 * 1024)
			fmt.Printf("  Disk:   [%s] %5.1f%% (%.1f/%.1f GB)\n",
				diskBar, r.DiskUsagePercent, diskUsedGB, diskTotalGB)
		} else {
			fmt.Println("  (no resource data available)")
		}
		fmt.Println()
	}

	return nil
}
