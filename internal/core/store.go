package core

import (
	"bytes"
	"crow/internal/datafile"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Store struct {
	dataFile   *datafile.Datafile
	KeyDir     KeyDir
	FileDir    datafile.FileDir
	BufferPool sync.Pool
	FileId     uint32
}

func createDirectory(directory string) error {
	if err := os.Mkdir(directory, os.ModePerm); err != nil {
		if os.IsExist(err) {
			if err := os.RemoveAll(directory); err != nil {
				return err
			}
			return createDirectory(directory)
		}
		return err
	}
	return nil
}

func New() (*Store, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	wd = filepath.Join(wd, "data")
	if err := createDirectory(wd); err != nil {
		return nil, err
	}

	df, err := datafile.New(filepath.Join(wd, "data_1.db"))
	if err != nil {
		fmt.Printf("Failed to create datafile: %s\n", err)
		return nil, err
	}

	return &Store{
		dataFile: df,
		BufferPool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
		KeyDir:  make(map[string]Meta),
		FileDir: make(map[uint32]*datafile.Datafile),
		FileId:  1,
	}, nil
}

func (s *Store) Get(key string) ([]byte, error) {
	meta := s.KeyDir[key]
	dataFile := s.FileDir[meta.FileId]

	fmt.Println(meta.Offset, meta.RecordSize)
	record, err := dataFile.Read(meta.Offset, meta.RecordSize)
	if err != nil {
		fmt.Printf("Failed to read: %s\n", err)
		return nil, err
	}

	fmt.Println(string(record))
	header := Header{}
	if err := header.decode(record); err != nil {
		fmt.Printf("Failed to decode %s", err)
		return nil, err
	}

	return record[meta.RecordSize-header.ValSize:], nil
}

func (s *Store) Set(key string, value []byte) error {
	header := Header{
		Timestamp: uint32(time.Now().Unix()),
		Crc:       crc32.ChecksumIEEE(value),
		KeySize:   uint32(len(key)),
		ValSize:   uint32(len(value)),
	}

	s.FileDir[s.FileId] = s.dataFile

	buffer := s.BufferPool.Get().(*bytes.Buffer)

	if err := header.encode(buffer); err != nil {
		return err
	}

	buffer.WriteString(key)
	buffer.Write(value)

	offset, err := s.dataFile.Append(buffer.Bytes())
	if err != nil {

	}
	//if !s.isValidOffset(offset) {
	//	return
	//}

	fmt.Println(offset, buffer.Len())

	s.KeyDir[key] = Meta{
		Timestamp:  header.Timestamp,
		Offset:     uint32(offset),
		RecordSize: uint32(buffer.Len()),
		FileId:     s.FileId,
	}

	me := s.KeyDir[key]
	fmt.Println(me.RecordSize)

	return nil
}

func (s *Store) isValidOffset(offset int) bool {
	return offset != datafile.InvalidOffset
}
