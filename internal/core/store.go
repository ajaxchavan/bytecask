package core

import (
	"bytes"
	"fmt"
	"go.uber.org/zap"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ajaxchavan/bytecask/internal/config"
	"github.com/ajaxchavan/bytecask/internal/datafile"
	"github.com/ajaxchavan/bytecask/internal/log"
)

var (
	RESP_OK           = []byte("OK")
	RESP_NIL          = []byte("(nil)")
	RESP_INTERNAL_ERR = []byte("internal_error")
	RESP_ONE          = []byte("1")
	RESP_ZERO         = []byte("0")
)

type Store struct {
	dataFile   *datafile.Datafile
	KeyDir     KeyDir
	FileDir    datafile.FileDir
	BufferPool sync.Pool
	FileId     int
	Log        log.Log
	cfg        config.Config
	sync.Mutex
}

func createDirectory(directory string) error {
	if err := os.Mkdir(directory, os.ModePerm); err != nil && !os.IsExist(err) {
		return err
	}
	return nil
}

func New(cfg config.Config, logger log.Log, hint bool) (*Store, error) {
	wd := filepath.Join(cfg.Path, cfg.Dir)

	err := createDirectory(wd)
	var number int

	store := Store{
		Log:     logger,
		cfg:     cfg,
		KeyDir:  make(map[string]*Meta),
		FileDir: make(map[int]*datafile.Datafile),
	}

	number, err = store.buildFileDir()
	if err != nil {
		const msg = "failed to build file directory"
		logger.Error(msg, zap.Error(err))
		return nil, fmt.Errorf(msg+": %w", err)
	}
	store.FileId = number

	if hint {
		if err := store.buildKeyDirWithHintFile(); err != nil {
			const msg = "failed to build key directory"
			logger.Error(msg, zap.Error(err))
			return nil, fmt.Errorf(msg+": %w", err)
		}
	} else {
		store.buildKeyDir()
	}

	number += 1

	df, err := datafile.New(filepath.Join(wd, fmt.Sprintf("data_%v.db", number)))
	if err != nil {
		const msg = "failed to create datafile"
		logger.Error(msg, zap.Error(err))
		return nil, fmt.Errorf(msg+": %w", err)
	}

	store.FileDir[number] = df

	// debug
	logger.Info("info", zap.Int("number", number))
	return &Store{
		dataFile: df,
		BufferPool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
		KeyDir:  store.KeyDir,
		FileDir: store.FileDir,
		FileId:  number,
		Log:     logger,
		cfg:     cfg,
	}, nil
}

func (s *Store) get(key string) []byte {
	s.Lock()
	meta := s.KeyDir[key]
	if meta == nil {
		s.Unlock()
		return RESP_NIL
	}
	dataFile := s.FileDir[meta.FileId]
	s.Unlock()

	// TODO(edge_case): we got the datafile and at the same time compaction deleted that datafile.
	object, err := dataFile.Read(meta.Offset, meta.ObjectSize)
	if err != nil {
		const msg = "failed to read data file"
		s.Log.Error(msg, zap.Error(err))
		return RESP_INTERNAL_ERR
	}

	header := Header{}
	if err := header.decode(object); err != nil {
		const msg = "failed to decode the record"
		s.Log.Error(msg, zap.Error(err))
		return RESP_INTERNAL_ERR
	}

	if header.ValSize == 0 {
		return RESP_NIL
	}

	return object[meta.ObjectSize-header.ValSize:]
}

func (s *Store) set(key string, value []byte) []byte {
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
		return RESP_INTERNAL_ERR
	}

	buffer.WriteString(key)
	buffer.Write(value)

	s.Lock()
	offset, err := s.dataFile.Append(buffer.Bytes())
	if err != nil {
		s.Unlock()
		const msg = "unable to append record"
		s.Log.Error(msg, zap.Error(err))
		return RESP_INTERNAL_ERR
	}

	s.KeyDir[key] = &Meta{
		Timestamp:  header.Timestamp,
		Offset:     uint32(offset),
		ObjectSize: uint32(buffer.Len()),
		FileId:     s.FileId,
	}
	s.Unlock()

	return RESP_OK
}

func (s *Store) del(key string) []byte {
	object := string(s.get(key))
	switch object {
	case string(RESP_NIL):
		return RESP_ZERO
	case string(RESP_INTERNAL_ERR):
		return RESP_INTERNAL_ERR
	}

	resp := s.set(key, nil)
	if string(resp) != string(RESP_OK) {
		return RESP_INTERNAL_ERR
	}

	return RESP_ONE
}

func (s *Store) isValidOffset(offset int) bool {
	return offset != datafile.InvalidOffset
}
