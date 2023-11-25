package datafile

import (
	"crow/internal/log"
	"os"
)

type Datafile struct {
	logger log.Log
	writer *os.File
	reader *os.File
	offset int
}

type FileDir map[uint32]*Datafile

const InvalidOffset = -1

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
		reader: reader,
		offset: 0,
	}, nil
}

// Flush ensures changes are persistently written to disk
func (d *Datafile) Flush() error {
	return d.writer.Sync()
}

func (d *Datafile) Append(data []byte) (int, error) {
	n, err := d.writer.Write(data)
	if err != nil {
		return InvalidOffset, err
	}

	offset := d.offset
	// update offset
	d.offset += n

	return offset, nil
}

func (d *Datafile) Read(off uint32, size uint32) ([]byte, error) {
	buff := make([]byte, size)

	if _, err := d.reader.ReadAt(buff, int64(off)); err != nil {
		return nil, err
	}

	return buff, nil
}
