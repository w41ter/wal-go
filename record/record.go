package record

type record struct {
	Crc32 uint32
	Index uint64
	Data  []byte
}

// Reset implements Messager interface.
func (r *record) Reset() {
	*r = record{}
}
