package clientcli

import (
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	pb "eddisonso.com/go-gfs/gen/master"
)

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}

func renderFileTable(w io.Writer, files []*pb.FileInfoResponse) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAMESPACE\tNAME\tCHUNKS\tSIZE")
	for _, f := range files {
		displayNamespace := f.Namespace
		if displayNamespace == "" {
			displayNamespace = "default"
		}
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d bytes\n",
			displayNamespace,
			f.Path,
			len(f.ChunkHandles),
			f.Size,
		)
	}
	tw.Flush()
}
