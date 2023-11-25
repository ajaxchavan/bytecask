package core

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

type Header struct {
	Crc       uint32
	Timestamp uint32
	KeySize   uint32
	ValSize   uint32
}

type Record struct {
	Header
	Key   string
	Value []byte
}

func (h *Header) encode(buffer *bytes.Buffer) error {
	return binary.Write(buffer, binary.BigEndian, h)
}

func (h *Header) decode(record []byte) error {
	return binary.Read(bytes.NewReader(record), binary.BigEndian, h)
}

func (r *Record) isValidCheckSum() bool {
	return crc32.ChecksumIEEE(r.Value) == r.Crc
}
