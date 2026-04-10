package logutil

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const (
	DefaultDir     = "logs"
	AppLogFileName = "app.log"
)

func OpenAppendFile(logDir, fileName string, perm os.FileMode) (*os.File, error) {
	dir := strings.TrimSpace(logDir)
	if dir == "" {
		dir = DefaultDir
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return os.OpenFile(filepath.Join(dir, fileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, perm)
}

func ConfigureStandardLogger(stdout io.Writer, logDir string) (*os.File, error) {
	if stdout == nil {
		stdout = os.Stdout
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(stdout)

	file, err := OpenAppendFile(logDir, AppLogFileName, 0o644)
	if err != nil {
		return nil, err
	}

	log.SetOutput(io.MultiWriter(stdout, file))
	return file, nil
}
