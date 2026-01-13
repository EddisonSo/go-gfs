package clientcli

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/term"
)

// TransferProgress tracks and displays transfer progress.
type TransferProgress struct {
	total      int64
	current    int64
	startTime  time.Time
	lastUpdate time.Time
	operation  string // "Uploading" or "Downloading"
	done       chan struct{}
	mu         sync.Mutex
	isTerminal bool
	termWidth  int
}

// NewTransferProgress creates a new progress tracker.
func NewTransferProgress(total int64, operation string) *TransferProgress {
	width := 40
	isTerminal := term.IsTerminal(int(os.Stderr.Fd()))
	if isTerminal {
		if w, _, err := term.GetSize(int(os.Stderr.Fd())); err == nil && w > 0 {
			width = w
		}
	}
	return &TransferProgress{
		total:      total,
		operation:  operation,
		startTime:  time.Now(),
		lastUpdate: time.Now(),
		done:       make(chan struct{}),
		isTerminal: isTerminal,
		termWidth:  width,
	}
}

// Update sets the current progress.
func (p *TransferProgress) Update(current int64) {
	p.mu.Lock()
	p.current = current
	p.mu.Unlock()
}

// Add increments the current progress.
func (p *TransferProgress) Add(delta int64) {
	p.mu.Lock()
	p.current += delta
	p.mu.Unlock()
}

// Start begins rendering the progress bar.
func (p *TransferProgress) Start() {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-p.done:
				p.render(true)
				return
			case <-ticker.C:
				p.render(false)
			}
		}
	}()
}

// Finish stops the progress bar and prints final state.
func (p *TransferProgress) Finish() {
	close(p.done)
	time.Sleep(50 * time.Millisecond)
}

func (p *TransferProgress) render(final bool) {
	p.mu.Lock()
	current := p.current
	total := p.total
	p.mu.Unlock()

	elapsed := time.Since(p.startTime).Seconds()
	if elapsed < 0.001 {
		elapsed = 0.001
	}

	percent := float64(0)
	if total > 0 {
		percent = float64(current) / float64(total) * 100
	}

	speed := float64(current) / elapsed
	speedStr := formatBytes(int64(speed)) + "/s"

	eta := ""
	if speed > 0 && current < total {
		remaining := float64(total-current) / speed
		eta = formatDuration(time.Duration(remaining) * time.Second)
	}

	barWidth := 30
	if p.termWidth < 80 {
		barWidth = 20
	}

	filled := int(percent / 100 * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	}

	bar := strings.Repeat("=", filled)
	if filled < barWidth && !final {
		bar += ">"
		bar += strings.Repeat(" ", barWidth-filled-1)
	} else if filled < barWidth {
		bar += strings.Repeat(" ", barWidth-filled)
	}

	currentStr := formatBytes(current)
	totalStr := formatBytes(total)

	line := fmt.Sprintf("\r%s [%s] %5.1f%% %s/%s %s",
		p.operation, bar, percent, currentStr, totalStr, speedStr)
	if eta != "" && !final {
		line += fmt.Sprintf(" ETA %s", eta)
	}

	if len(line) < p.termWidth {
		line += strings.Repeat(" ", p.termWidth-len(line))
	}

	if p.isTerminal {
		fmt.Fprint(os.Stderr, line)
		if final {
			fmt.Fprintln(os.Stderr)
		}
	} else if final {
		fmt.Fprintf(os.Stderr, "%s: %s (%s)\n", p.operation, totalStr, speedStr)
	}
}

// ProgressWriter wraps a writer and updates progress.
type ProgressWriter struct {
	w        io.Writer
	progress *TransferProgress
}

func (pw *ProgressWriter) Write(p []byte) (int, error) {
	n, err := pw.w.Write(p)
	if n > 0 {
		pw.progress.Add(int64(n))
	}
	return n, err
}

// ProgressReader wraps a reader and updates progress.
type ProgressReader struct {
	r        io.Reader
	progress *TransferProgress
}

func (pr *ProgressReader) Read(p []byte) (int, error) {
	n, err := pr.r.Read(p)
	if n > 0 {
		pr.progress.Add(int64(n))
	}
	return n, err
}
