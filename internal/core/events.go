package core

import (
	"encoding/gob"
	"go.uber.org/zap"
	"os"
	"path/filepath"
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
	if err := encoder.Encode(s.KeyDir); err != nil {
		return err
	}
	return nil
}
