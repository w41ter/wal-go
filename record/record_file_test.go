package record

import (
	"bytes"
	"os"
	"testing"
)

func TestFile_Full(t *testing.T) {
	filename := "/tmp/xxxxx"
	file, err := CreateFile(filename)
	if err != nil {
		t.Error(err)
	}

	size := recordFileSize
	data := make([]byte, size-100)
	if err := file.Write(1, data); err != nil {
		t.Error(err)
	}

	if file.Full() {
		t.Errorf("want no full, but full")
	}

	data = make([]byte, 1)
	if err := file.Write(2, data); err != nil {
		t.Error(err)
	}

	if !file.Full() {
		t.Errorf("want full, but no full")
	}
	os.Remove(filename)
}

func TestFile_Restore(t *testing.T) {
	tests := []struct {
		idx  uint64
		data []byte
	}{
		{1, []byte{0x1}},
		{2, []byte{0x2}},
		{3, []byte{0x3, 0x4, 0x5}},
		{9, []byte{0x6}},
		{10, make([]byte, 1000)},
		{11, []byte{0x1}},
		{12, []byte{0x2}},
		{13, []byte{0x3, 0x4, 0x5}},
		{19, []byte{0x6}},
	}

	filename := "/tmp/xxx"
	file, err := CreateFile(filename)
	if err != nil {
		t.Error(err)
		return
	}

	for i, test := range tests {
		if err = file.Write(test.idx, test.data); err != nil {
			t.Errorf("#%d: write file error", i)
		}
	}
	file.Close()

	getTest := func(idx uint64) *struct {
		idx  uint64
		data []byte
	} {
		for _, test := range tests {
			if test.idx == idx {
				return &test
			}
		}
		return nil
	}

	file, err = RestoreFile(filename, 2, func(index uint64, data []byte) error {
		if index < 2 {
			t.Errorf("get old index")
		}

		test := getTest(index)
		if test == nil {
			t.Errorf("idx: %d, could not found test", index)
		}

		if !bytes.Equal(data, test.data) {
			t.Errorf("idx: %d, want not equals to get", index)
		}
		return nil
	})

	if err != nil {
		t.Error(err)
	} else {
		file.Close()
	}

	os.Remove(filename)
}
