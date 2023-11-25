package core

type KeyDir map[string]Meta

type Meta struct {
	Timestamp  uint32
	Offset     uint32
	RecordSize uint32
	FileId     uint32
}
