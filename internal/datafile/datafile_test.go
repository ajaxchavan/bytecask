package datafile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppend(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	df, err := NewDatafile(tmpDir)
	if err != nil {
		t.Fatalf("failed to create new Datafile instance: %v", err)
	}
	defer func() {
		df.writer.Close()
		df.Reader.Close()
	}()

	// Append some test data
	data := []byte("test data")
	offset, err := df.Append(data)
	if err != nil {
		t.Fatalf("error appending data: %v", err)
	}

	if df.offset != len(data) {
		t.Errorf("expected offset %v, got %v", len(data), offset)
	}

	// Test appending empty data
	offsetEmpty, err := df.Append([]byte{})
	if err == nil || err != ErrEmptyData {
		t.Error("expected error for appending empty data")
	}
	if offsetEmpty != InvalidOffset {
		t.Errorf("expected offset %v, got %v", InvalidOffset, offsetEmpty)
	}
}

func TestRead(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	df, err := NewDatafile(tmpDir)
	if err != nil {
		t.Fatalf("failed to create new Datafile: %v", err)
	}
	defer func() {
		df.writer.Close()
		df.Reader.Close()
	}()

	// Append some test data
	data := []byte("test data")
	offset, err := df.Append(data)
	if err != nil {
		t.Fatalf("error appending data: %v", err)
	}

	// Test Read function
	readData, err := df.Read(uint32(offset), uint32(len(data)))
	if err != nil {
		t.Fatalf("error reading data: %v", err)
	}

	if string(readData) != string(data) {
		t.Errorf("expected %s, got %s", string(data), string(readData))
	}
}

func TestIsFull(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	df, err := NewDatafile(tmpDir)
	if err != nil {
		t.Fatalf("failed to create new Datafile: %v", err)
	}
	defer func() {
		df.writer.Close()
		df.Reader.Close()
	}()

	// Append some data to make file nearly full
	data := make([]byte, dataFileSizeMax-1)
	_, err = df.Append(data)
	if err != nil {
		t.Fatalf("error appending data: %v", err)
	}

	// Test IsFull function
	if !df.IsFull() {
		t.Error("expected Datafile to be full")
	}
}

func NewDatafile(dir string) (*Datafile, error) {
	// Define a file path within the temporary directory
	filePath := filepath.Join(dir, "test_data.db")

	// Create a new Datafile instance
	df, err := New(filePath)
	if err != nil {
		return nil, err
	}
	return df, nil
}
