package core

import (
	"encoding/gob"
	"fmt"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/ajaxchavan/crow/internal/datafile"
)

const (
	hintFile          = "key_hint.db"
	headerSize uint32 = 16
	errLimit   uint32 = 5
)

var (
	// fileRegex is the regex for a file.
	// A valid file is in the format of: data_[0-9].db
	fileRegex = regexp.MustCompile(`data_([0-9]+)\.db`)
)

// buildStale
func (s *Store) buildFileDir() (int, error) {
	var (
		filePath   string
		reader     *os.File
		number     int
		fileId     int    = 0
		err        error  = nil
		errCounter uint32 = 0
	)

	data, err := ioutil.ReadDir(s.cfg.Dir)
	if err != nil {
		const msg = "failed read data directory"
		s.Log.Error(msg, zap.Error(err))
		return fileId, fmt.Errorf(msg+": %w", err)
	}

	for _, file := range data {
		if file.Name() == hintFile {
			continue
		}

		matches := fileRegex.FindStringSubmatch(file.Name())
		if len(matches) < 1 {
			const msg = "no number found in the file name"
			s.Log.Error(msg, zap.Error(err), zap.String("unrecognised file", file.Name()))
			continue
		}
		numberStr := matches[1]
		number, err = strconv.Atoi(numberStr)
		if err != nil {
			const msg = "number provided for file is not a valid integer"
			s.Log.Error(msg, zap.Error(err), zap.String("number", numberStr))
			continue
		}
		if number > fileId {
			fileId = number
		}

		errCounter = 0
		for {
			if file.Mode().IsRegular() {
				filePath = filepath.Join(s.cfg.Dir, file.Name())
				reader, err = os.Open(filePath)
				if err != nil {
					const msg = "failed to open file"
					s.Log.Error(msg, zap.Error(err))
					if errCounter >= errLimit {
						break
					}
					errCounter += 1
					continue
				}

				s.FileDir[number] = &datafile.Datafile{
					Reader: reader,
				}
				break
			}
		}

		if errCounter > errLimit {
			const msg = "failed creat a reader for file"
			s.Log.Error(msg, zap.Error(err), zap.String("file", file.Name()))
			return 0, fmt.Errorf(msg+": %w", err)
		}
	}

	return fileId, nil
}

func (s *Store) buildKeyDirWithHintFile() error {
	fpath := filepath.Join(s.cfg.Dir, hintFile)
	file, err := os.Open(fpath)
	if err != nil {
		const msg = "failed to open hint file"
		s.Log.Error(msg, zap.Error(err))
		return fmt.Errorf(msg+": %w", err)
	}

	encoder := gob.NewDecoder(file)
	if err := encoder.Decode(&s.KeyDir); err != nil {
		const msg = "failed to decode keydir"
		s.Log.Error(msg, zap.Error(err))
		return fmt.Errorf(msg+": %w", err)
	}

	return nil
}

func (s *Store) buildKeyDir() {
	var (
		err        error  = nil
		offset     uint32 = 0
		headerObj  []byte
		header     Header
		object     []byte
		objectSize uint32
		errCounter uint32 = 0
		key        string
	)

	for fileId := 1; fileId <= s.FileId; fileId++ {
		errCounter = 0
		dt := s.FileDir[fileId]
		offset = 0
		for {
			headerObj, err = dt.Read(offset, headerSize)
			if err != nil {
				if err == io.EOF || errCounter > errLimit {
					break
				}
				s.Log.Error("failed to read datafile for header", zap.Error(err), zap.Uint32("error counter", errCounter))
				errCounter += 1
				continue
			}

			if err = header.decode(headerObj); err != nil {
				const msg = "failed to decode the header"
				s.Log.Error(msg, zap.Error(err))
				if errCounter > errLimit {
					break
				}
				errCounter += 1
				continue
			}

			if header.Timestamp == 0 {
				offset += headerSize
				continue
			}

			objectSize = headerSize + header.KeySize + header.ValSize
			object, err = dt.Read(offset, objectSize)
			if err != nil {
				if err == io.EOF || errCounter > errLimit {
					break
				}
				s.Log.Error("failed to read datafile for header", zap.Error(err), zap.Uint32("error counter", errCounter))
				errCounter += 1
				continue
			}

			key = string(object[headerSize : headerSize+header.KeySize])

			s.KeyDir[key] = &Meta{
				Timestamp:  header.Timestamp,
				Offset:     offset,
				ObjectSize: objectSize,
				FileId:     fileId,
			}
			offset += objectSize
			errCounter = 0
		}
	}
}
