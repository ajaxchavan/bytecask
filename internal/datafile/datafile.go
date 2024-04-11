package datafile

import (
	"fmt"
	"github.com/ajaxchavan/bytecask/internal/log"
	"os"
	"path/filepath"
)

type Datafile struct {
	logger log.Log
	writer *os.File
	Reader *os.File
	offset int
}

type FileDir map[int]*Datafile

const (
	InvalidOffset = -1

	// debug
	dataFileSizeMax = 128
)

// New creates a new Datafile instance with the given file path.
func New(filePath string) (*Datafile, error) {
	writer, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}

	reader, err := os.Open(filePath)
	if err != nil {
		writer.Close()
		return nil, err
	}

	return &Datafile{
		//logger: logger,
		writer: writer,
		Reader: reader,
		offset: 0,
	}, nil
}

// GetDatafile returns the file path for a Datafile identified by fileId.
// It concatenates the provided filePath with a formatted string containing the fileId and returns the result.
func GetDatafile(filePath string, fileId int) string {
	return filepath.Join(filePath, fmt.Sprintf("data_%v.db", fileId))
}

// Flush ensures changes are persistently written to disk
func (d *Datafile) Flush() error {
	return d.writer.Sync()
}

// Append appends the provided data slice to the Datafile, returning the offset at which the data was written.
// It updates the internal offset of the Datafile instance to reflect the new position after the append operation.
func (d *Datafile) Append(data []byte) (int, error) {
	if data == nil || len(data) == 0 {
		return InvalidOffset, ErrEmptyData
	}
	n, err := d.writer.Write(data)
	if err != nil {
		return InvalidOffset, err
	}

	offset := d.offset
	// update the datafile's internal offset
	d.offset += n

	return offset, nil
}

// Read reads data from the Datafile starting at the specified offset (off)
// and reads the specified number of bytes (size)
func (d *Datafile) Read(off uint32, size uint32) ([]byte, error) {
	buff := make([]byte, size)

	if _, err := d.Reader.ReadAt(buff, int64(off)); err != nil {
		return nil, err
	}

	return buff, nil
}

// IsFull checks whether the data file associated with the Datafile instance
// is nearly full (95%)
func (d *Datafile) IsFull() bool {
	frac := float32(d.offset) / float32(dataFileSizeMax)
	return frac >= 0.95
}
