package logutil

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenAppendFileCreatesDirectoryAndAppends(t *testing.T) {
	logDir := filepath.Join(t.TempDir(), "nested", "logs")

	first, err := OpenAppendFile(logDir, "app.log", 0o644)
	if err != nil {
		t.Fatalf("OpenAppendFile first call returned error: %v", err)
	}
	if _, err := first.WriteString("first line\n"); err != nil {
		t.Fatalf("first WriteString returned error: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close returned error: %v", err)
	}

	second, err := OpenAppendFile(logDir, "app.log", 0o644)
	if err != nil {
		t.Fatalf("OpenAppendFile second call returned error: %v", err)
	}
	if _, err := second.WriteString("second line\n"); err != nil {
		t.Fatalf("second WriteString returned error: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(logDir, "app.log"))
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	got := string(data)
	if !strings.Contains(got, "first line\nsecond line\n") {
		t.Fatalf("app log = %q, want both appended lines", got)
	}
}

func TestConfigureStandardLoggerWritesToStdoutAndFile(t *testing.T) {
	prevWriter := log.Writer()
	prevFlags := log.Flags()
	t.Cleanup(func() {
		log.SetOutput(prevWriter)
		log.SetFlags(prevFlags)
	})

	var stdout bytes.Buffer
	logDir := filepath.Join(t.TempDir(), "logs")

	logFile, err := ConfigureStandardLogger(&stdout, logDir)
	if err != nil {
		t.Fatalf("ConfigureStandardLogger returned error: %v", err)
	}
	t.Cleanup(func() {
		if err := logFile.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	})

	log.Print("dual write check")

	fileData, err := os.ReadFile(filepath.Join(logDir, AppLogFileName))
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}

	if !strings.Contains(stdout.String(), "dual write check") {
		t.Fatalf("stdout log = %q, want entry", stdout.String())
	}
	if !strings.Contains(string(fileData), "dual write check") {
		t.Fatalf("file log = %q, want entry", string(fileData))
	}
}
