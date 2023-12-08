package core

import (
	"bytes"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ajaxchavan/crow/internal/config"
	"github.com/ajaxchavan/crow/internal/datafile"
	"github.com/ajaxchavan/crow/internal/log"
)

type Store struct {
	dataFile   *datafile.Datafile
	KeyDir     KeyDir
	FileDir    datafile.FileDir
	BufferPool sync.Pool
	FileId     uint32
	Log        log.Log
	cfg        config.Config
	stale      datafile.StaleDir
	sync.Mutex
}

func createDirectory(directory string) error {
	if err := os.Mkdir(directory, os.ModePerm); err != nil {
		return err
	}
	return nil
}

func New(cfg config.Config, logger log.Log) (*Store, error) {
	wd, err := os.Getwd()
	if err != nil {
		const msg = "failed to get current directory"
		logger.Error(msg, zap.Error(err))
		return nil, err
	}
	wd = filepath.Join(wd, cfg.Dir)

	var stale datafile.StaleDir
	var keyDir KeyDir
	var fileDir map[uint32]*datafile.Datafile

	err = createDirectory(wd)
	var number uint32

	switch {
	case err == nil:
		keyDir = make(map[string]Meta)
		fileDir = make(map[uint32]*datafile.Datafile)
	case errors.Is(err, os.ErrExist):
		fileDir, number, err = buildStale(cfg.Dir, logger)
		if err != nil {
			const msg = "failed to build stale data"
			logger.Error(msg, zap.Error(err))
			return nil, err
		}
		keyDir, err = buildKeyDir(cfg.Dir, logger)
		if err != nil {
			const msg = "failed to build key directory"
			logger.Error(msg, zap.Error(err))
			return nil, err
		}
	default:
		const msg = "failed to create data directory"
		logger.Error(msg, zap.Error(err))
		return nil, err
	}
	number += 1

	df, err := datafile.New(filepath.Join(wd, fmt.Sprintf("data_%v.db", number)))
	if err != nil {
		const msg = "Failed to create datafile"
		logger.Error(msg, zap.Error(err))
		return nil, err
	}

	// debug
	logger.Info("info", zap.Uint32("number", number))
	return &Store{
		dataFile: df,
		BufferPool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
		KeyDir:  keyDir,
		FileDir: fileDir,
		FileId:  number,
		Log:     logger,
		cfg:     cfg,
		stale:   stale,
	}, nil
}

func (s *Store) Get(key string) ([]byte, error) {
	s.Lock()
	meta := s.KeyDir[key]
	if meta.RecordSize == 0 {
		s.Unlock()
		return nil, fmt.Errorf("key doesn't exist")
	}
	dataFile := s.FileDir[meta.FileId]
	s.Unlock()

	// TODO(edge_case): we got the datafile and at the same time compaction deleted that datafile.
	record, err := dataFile.Read(meta.Offset, meta.RecordSize)
	if err != nil {
		const msg = "failed to read data file"
		s.Log.Error(msg, zap.Error(err))
		return nil, err
	}

	header := Header{}
	if err := header.decode(record); err != nil {
		const msg = "failed to decode the record"
		s.Log.Error(msg, zap.Error(err))
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
	buffer := s.BufferPool.Get().(*bytes.Buffer)
	defer s.BufferPool.Put(buffer)
	defer buffer.Reset()

	if err := header.encode(buffer); err != nil {
		const msg = "unable to encode record"
		s.Log.Error(msg, zap.Error(err))
		return err
	}

	buffer.WriteString(key)
	buffer.Write(value)

	s.Lock()
	s.FileDir[s.FileId] = s.dataFile
	offset, err := s.dataFile.Append(buffer.Bytes())
	if err != nil {
		s.Unlock()
		const msg = "unable to append record"
		s.Log.Error(msg, zap.Error(err))
		return err
	}

	s.KeyDir[key] = Meta{
		Timestamp:  header.Timestamp,
		Offset:     uint32(offset),
		RecordSize: uint32(buffer.Len()),
		FileId:     s.FileId,
	}
	s.Unlock()

	return nil
}

func (s *Store) Del(key string) error {
	meta := s.KeyDir[key]
	if meta.RecordSize == 0 {
		return fmt.Errorf("key doesn't exist")
	}
	delete(s.KeyDir, key)
	return nil
}

func (s *Store) isValidOffset(offset int) bool {
	return offset != datafile.InvalidOffset
}
