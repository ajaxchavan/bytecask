package core

import (
	"crow/internal/datafile"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
)

const (
	hintFile = "key_hint.db"
)

func buildStale(dir string) (map[uint32]*datafile.Datafile, uint32, error) {
	data, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}

	var filePath string
	var reader *os.File
	var number int
	staleDir := make(map[uint32]*datafile.Datafile)
	for _, file := range data {
		if file.Name() == "key_hint.db" {
			continue
		}

		if file.Mode().IsRegular() {
			filePath = filepath.Join(dir, file.Name())
			reader, err = os.Open(filePath)
			if err != nil {
				return nil, 0, err
			}
			re := regexp.MustCompile(`(\d+)`)
			matches := re.FindStringSubmatch(file.Name())
			if len(matches) < 1 {
				return nil, 0, fmt.Errorf("no number found in the file name")
			}
			numberStr := matches[1]
			number, err = strconv.Atoi(numberStr)
			if err != nil {
				return nil, 0, err
			}
			staleDir[uint32(number)] = &datafile.Datafile{
				Reader: reader,
			}
		}
	}
	return staleDir, uint32(number), nil
}

func buildKeyDir(dir string) (map[string]Meta, error) {
	fpath := filepath.Join(dir, hintFile)
	file, err := os.Open(fpath)
	if err != nil {
		return nil, err
	}

	encoder := gob.NewDecoder(file)
	keyDir := make(map[string]Meta)
	if err := encoder.Decode(&keyDir); err != nil {
		return nil, err
	}
	return keyDir, nil
}
