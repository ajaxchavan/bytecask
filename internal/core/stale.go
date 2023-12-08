package core

import (
	"encoding/gob"
	"fmt"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/ajaxchavan/crow/internal/datafile"
	"github.com/ajaxchavan/crow/internal/log"
)

const (
	hintFile = "key_hint.db"
)

func buildStale(dir string, logger log.Log) (map[uint32]*datafile.Datafile, uint32, error) {
	data, err := ioutil.ReadDir(dir)
	if err != nil {
		const msg = "failed read data directory"
		logger.Error(msg, zap.Error(err))
		return nil, 0, err
	}

	var filePath string
	var reader *os.File
	var number int
	staleDir := make(map[uint32]*datafile.Datafile)
	for _, file := range data {
		if file.Name() == hintFile {
			continue
		}

		if file.Mode().IsRegular() {
			filePath = filepath.Join(dir, file.Name())
			reader, err = os.Open(filePath)
			if err != nil {
				const msg = "failed to open file"
				logger.Error(msg, zap.Error(err))
				return nil, 0, err
			}
			re := regexp.MustCompile(`(\d+)`)
			matches := re.FindStringSubmatch(file.Name())
			if len(matches) < 1 {
				const msg = "unrecognised filed"
				logger.Error(msg, zap.Error(err), zap.String("file", file.Name()))
				return nil, 0, fmt.Errorf("no number found in the file name")
			}
			numberStr := matches[1]
			number, err = strconv.Atoi(numberStr)
			if err != nil {
				const msg = "number provided for file is not a valid integer"
				logger.Error(msg, zap.Error(err), zap.String("number", numberStr))
				return nil, 0, err
			}
			staleDir[uint32(number)] = &datafile.Datafile{
				Reader: reader,
			}
		}
	}
	return staleDir, uint32(number), nil
}

func buildKeyDir(dir string, logger log.Log) (map[string]Meta, error) {
	fpath := filepath.Join(dir, hintFile)
	file, err := os.Open(fpath)
	if err != nil {
		return nil, err
	}

	encoder := gob.NewDecoder(file)
	keyDir := make(map[string]Meta)
	if err := encoder.Decode(&keyDir); err != nil {
		const msg = "failed to decode keydir"
		logger.Error(msg, zap.Error(err))
		return nil, err
	}
	return keyDir, nil
}
