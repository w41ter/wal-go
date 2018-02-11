package file

import (
	"io"
	"math/rand"
	"testing"
)

type MemoryFile struct {
	content []byte
	offset  int
}

func MakeMemoryFile(bytes []byte) *MemoryFile {
	content := make([]byte, len(bytes))
	copy(content, bytes)
	return &MemoryFile{
		content: content,
		offset:  0,
	}
}

func (file *MemoryFile) Write(bytes []byte) (int, error) {
	file.content = append(file.content, bytes...)
	file.offset = len(file.content)
	return len(bytes), nil
}

func (file *MemoryFile) Read(bytes []byte) (int, error) {
	if file.offset == len(file.content) {
		return 0, io.EOF
	}

	end := min(len(file.content)-file.offset, len(bytes))
	copy(bytes[:end], file.content[file.offset:])
	file.offset += end
	return end, nil
}

func randBytes(size int) []byte {
	bytes := make([]byte, size)
	for i := 0; i < size; i++ {
		bytes[i] = byte('a' + rand.Intn(26))
	}
	return bytes
}

func compare(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestBuffer_Read(t *testing.T) {
test:
	for i := 0; i < 10; i++ {
		bytes := randBytes(rand.Intn(1024 * 100))
		file := MakeMemoryFile(bytes)
		buf := BufferCreate(file)
		length := len(bytes)
		wbuf := make([]byte, length)
		roff := 0
		for length > 0 {
			num := rand.Intn(1024 * 5)
			rd := min(num, length)
			if err := buf.Read(wbuf[roff : roff+rd]); err != nil && err != io.EOF {
				t.Error(err)
				continue test
			}
			if !compare(bytes[roff:roff+rd], wbuf[roff:roff+rd]) {
				t.Errorf("read file not equal: want %s, get: %s",
					bytes[roff:roff+rd], wbuf[roff:roff+rd])
				continue test
			}
			roff += rd
			length -= rd
		}
	}
}

func TestBuffer_Write(t *testing.T) {
test:
	for i := 0; i < 10; i++ {
		bytes := randBytes(rand.Intn(1024 * 100))
		file := MakeMemoryFile([]byte{})
		buf := BufferCreate(file)
		length := len(bytes)
		offset := 0
		for offset < length {
			num := rand.Intn(1024 * 5)
			rd := min(num, length-offset)
			if err := buf.Write(bytes[offset : offset+rd]); err != nil {
				t.Error(err)
				continue test
			}
			offset += rd
		}

		buf.Flush()

		wbuf := make([]byte, length)
		file.offset = 0

		if buf.Read(wbuf) != nil || !compare(bytes, wbuf) {
			t.Errorf("read file not equal: want %s, get: %s", wbuf, bytes)
			continue
		}
	}
}
