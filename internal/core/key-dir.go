package core

type KeyDir map[string]*Meta

type Meta struct {
	Timestamp  uint32
	Offset     uint32
	ObjectSize uint32
	FileId     int
}
