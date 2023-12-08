package core

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/ajaxchavan/crow/internal/datafile"
)

const (
	tempDir  = ".temp_data"
	tempDir2 = ".temp2_data"
)

func (s *Store) Shutdown() {
	if err := s.flush(); err != nil {
		s.Log.Error("failed to flush keyDir to disk while shutting down", zap.Error(err))
	}
	if err := s.dataFile.Flush(); err != nil {
		s.Log.Error("failed to flush datafile to disk while shutting down", zap.Error(err))
	}
}

func (s *Store) flush() error {
	fpath := filepath.Join(s.cfg.Opts.Dir, hintFile)
	writer, err := os.OpenFile(fpath, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}

	encoder := gob.NewEncoder(writer)
	s.Lock()
	defer s.Unlock()
	if err := encoder.Encode(s.KeyDir); err != nil {
		return err
	}
	return nil
}

func (s *Store) AsyncFlush(wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(s.cfg.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.flush(); err != nil {
				const msg = "failed to flush keyDir to disk"
				s.Log.Error(msg, zap.Error(err))
			}
			if err := s.dataFile.Flush(); err != nil {
				const msg = "failed to flush datafile to disk"
				s.Log.Error(msg, zap.Error(err))
			}
			// TODO:
			// case <-ctx.Done():
			//	s.Log.Info("canceling async flush run")
			//	return
		}
	}
}

func (s *Store) Compact(wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(s.cfg.MergeInterval)

	for {
		select {
		case <-ticker.C:
			s.compact()
		}
	}
}

func (s *Store) compact() {
	if s.FileId < 2 {
		s.Log.Info("only 1 datafile exist, skipping compaction")
		return
	}
	s.Log.Info("compaction started...")

	cwd, err := os.Getwd()
	if err != nil {
		const msg = "failed to get current directory"
		s.Log.Error(msg, zap.Error(err))
		return
	}
	wd := filepath.Join(cwd, tempDir)
	if err := createDirectory(wd); err != nil && !os.IsExist(err) {
		const msg = "failed to create a temp directory for compaction"
		s.Log.Error(msg, zap.Error(err))
		return
	}

	dt, err := datafile.New(filepath.Join(tempDir, fmt.Sprintf("data_%v.db", 1)))
	if err != nil {
		const msg = "failed to create a datafile for compaction"
		s.Log.Error(msg, zap.Error(err))
		s.removeTemp(wd)
		return
	}
	nKeyDir := make(map[string]Meta)

	s.Lock()
	tempKeyDir := s.KeyDir
	s.Unlock()

	for key, meta := range tempKeyDir {
		s.Log.Info("compaction", zap.String("key", key))
		if meta.RecordSize == 0 {
			s.Log.Error("key doesn't exist", zap.String("key", key))
			continue
		}
		dataFile := s.FileDir[meta.FileId]

		record, err := dataFile.Read(meta.Offset, meta.RecordSize)
		if err != nil {
			const msg = "failed to read data file"
			s.Log.Error(msg, zap.Error(err))
			s.removeTemp(wd)
			return
		}
		offset, err := dt.Append(record)
		if err != nil {
			const msg = "unable to append record"
			s.Log.Error(msg, zap.Error(err))
			s.removeTemp(wd)
			return
		}

		nKeyDir[key] = Meta{
			Timestamp:  meta.Timestamp,
			Offset:     uint32(offset),
			RecordSize: meta.RecordSize,
			FileId:     1,
		}
	}

	fpath := filepath.Join(cwd, hintFile)
	writer, err := os.OpenFile(fpath, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		const msg = "unable to open hint file"
		s.Log.Error(msg, zap.Error(err))
		s.removeTemp(wd)
		return
	}

	encoder := gob.NewEncoder(writer)
	if err := encoder.Encode(nKeyDir); err != nil {
		const msg = "unable to open hint file"
		s.Log.Error(msg, zap.Error(err))
		s.removeTemp(wd)
		return
	}

	// lock
	s.Lock()

	if err := os.Rename(s.cfg.Dir, filepath.Join(cwd, tempDir2)); err != nil {
		s.Unlock()
		const msg = "unable to rename data directory to temp directory"
		s.Log.Error(msg, zap.Error(err))
		s.removeTemp(wd)
		return
	}

	// TODO: the data directory is already been renamed and if we get error here we need to rename it to data directory
	if err := os.Rename(wd, filepath.Join(cwd, ".data")); err != nil {
		s.Unlock()
		const msg = "unable to rename temp directory to data directory"
		s.Log.Error(msg, zap.Error(err))
		s.removeTemp(wd)
		return
	}

	s.KeyDir = nKeyDir
	s.FileDir = make(datafile.FileDir)
	s.FileDir[1] = dt
	nDatafile, err := datafile.New(filepath.Join(s.cfg.Dir, fmt.Sprintf("data_%v.db", 2)))
	if err != nil {
		const msg = "failed to create a new datafile"
		s.Log.Error(msg, zap.Error(err))
		s.dataFile = dt
		s.FileId = 1
	} else {
		s.dataFile = nDatafile
		s.FileId = 2
	}

	// debug
	time.Sleep(time.Minute * 2)

	s.Unlock()
	s.Log.Info("compaction done...")

	//TODO: check if s.cfg.Dir is pointing to the right directory
	s.removeTemp(filepath.Join(cwd, tempDir2))
}

func (s *Store) removeTemp(wd string) {
	if err := os.RemoveAll(wd); err != nil {
		const msg = "failed remove all the temp files related to compaction"
		s.Log.Error(msg, zap.Error(err))
	}
}
