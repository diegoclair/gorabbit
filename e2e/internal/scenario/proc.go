package scenario

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/diegoclair/gorabbit/e2e/internal/appkit"
)

const (
	startTimeout = 30 * time.Second
	stopTimeout  = 15 * time.Second
)

var httpClient = &http.Client{Timeout: 15 * time.Second}

// Proc is one harness application running as its own operating-system process,
// which is the whole point of this harness: nothing here shares memory.
type Proc struct {
	Name    string
	BaseURL string
	LogPath string

	cmd     *exec.Cmd
	done    chan struct{}
	waitErr error

	mu      sync.Mutex
	stopped bool
}

func startProc(name, logDir, binary string, args []string) (*Proc, error) {
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, err
	}

	logPath := filepath.Join(logDir, name+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(binary, args...)
	cmd.Stderr = logFile

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		logFile.Close()
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, fmt.Errorf("starting %s (%s): %w", name, binary, err)
	}

	p := &Proc{Name: name, LogPath: logPath, cmd: cmd, done: make(chan struct{})}

	addresses := make(chan string, 1)
	readerDone := make(chan struct{})

	go func() {
		defer close(readerDone)
		scan(stdout, logFile, addresses)
	}()

	go func() {
		<-readerDone
		p.waitErr = cmd.Wait()
		logFile.Close()
		close(p.done)
	}()

	select {
	case addr := <-addresses:
		p.BaseURL = "http://" + addr
		return p, nil
	case <-p.done:
		return nil, fmt.Errorf("%s exited before announcing its address (%v); log: %s", name, p.waitErr, logPath)
	case <-time.After(startTimeout):
		_ = p.Kill()
		return nil, fmt.Errorf("%s did not announce an address within %s; log: %s", name, startTimeout, logPath)
	}
}

func scan(stdout io.Reader, logFile io.Writer, addresses chan<- string) {
	scanner := bufio.NewScanner(stdout)
	announced := false

	for scanner.Scan() {
		line := scanner.Text()
		fmt.Fprintln(logFile, line)

		if !announced && strings.HasPrefix(line, appkit.ListenAnnouncement+" ") {
			announced = true
			addresses <- strings.TrimSpace(strings.TrimPrefix(line, appkit.ListenAnnouncement))
		}
	}
}

func (p *Proc) Running() bool {
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

// Kill takes the process away with no chance to close its client, which is the
// only way to ask what survives a crash.
func (p *Proc) Kill() error {
	p.mu.Lock()
	p.stopped = true
	p.mu.Unlock()

	if err := p.cmd.Process.Signal(syscall.SIGKILL); err != nil && p.Running() {
		return err
	}

	select {
	case <-p.done:
		return nil
	case <-time.After(stopTimeout):
		return fmt.Errorf("%s did not die within %s of SIGKILL", p.Name, stopTimeout)
	}
}

// Stop is the orderly shutdown a deploy does, so the client releases what it
// holds before the process goes.
func (p *Proc) Stop() error {
	p.mu.Lock()
	already := p.stopped
	p.stopped = true
	p.mu.Unlock()

	if already || !p.Running() {
		return nil
	}

	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return err
	}

	select {
	case <-p.done:
		return nil
	case <-time.After(stopTimeout):
		return p.Kill()
	}
}

// ExitStatus is what the operating system says about the death, so a step can
// show that a kill really was a kill.
func (p *Proc) ExitStatus() string {
	select {
	case <-p.done:
		if p.waitErr == nil {
			return "exited cleanly"
		}
		return p.waitErr.Error()
	default:
		return "running"
	}
}

func (p *Proc) GetJSON(path string, out any) error {
	resp, err := httpClient.Get(p.BaseURL + path)
	if err != nil {
		return fmt.Errorf("%s GET %s: %w", p.Name, path, err)
	}

	return decode(resp, p.Name, path, out)
}

func (p *Proc) PostJSON(path string, body, out any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := httpClient.Post(p.BaseURL+path, "application/json", bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("%s POST %s: %w", p.Name, path, err)
	}

	return decode(resp, p.Name, path, out)
}

func decode(resp *http.Response, name, path string, out any) error {
	defer resp.Body.Close()

	answer, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: status %d: %s", name, path, resp.StatusCode, bytes.TrimSpace(answer))
	}

	if out == nil {
		return nil
	}

	return json.Unmarshal(answer, out)
}
