package wal

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/thinkermao/wal-go/file"
)

func createTmpDir(t *testing.T) string {
	path := "/tmp/wal"
	if err := os.MkdirAll(path, 0766); err != nil {
		t.Fatal(err)
	}
	return path
}

func createFileAndClose(t *testing.T, path string) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0766)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if !file.IsExists(path) {
		t.Fatal("crete file ", path, " failed")
	}
}

func TestNew(t *testing.T) {
	p := createTmpDir(t)
	defer os.RemoveAll(p)

	w, err := Create(p, 0)
	if err != nil {
		t.Fatal(err)
	}
	if g := filepath.Base(w.back().filename); g != walName(0, 0) {
		t.Errorf("name = %+v, want %+v", g, walName(0, 0))
	}
	w.Close()

	if !file.IsExists(w.back().filename) {
		t.Errorf("file: %s not exists", w.back().filename)
	}
}

func TestOpenAtIndex(t *testing.T) {
	type testParam struct {
		seq, idx uint64
		at       uint64
		werr     error
	}
	tests := []testParam{
		// idx eq
		{0, 0, 0, nil},
		// idx great
		{2, 10, 11, nil},
		// idx less
		{0, 5, 1, errFileNotFound},
	}

	for i, test := range tests {
		func(i int, test testParam) {
			dir := createTmpDir(t)
			defer os.RemoveAll(dir)

			filename := walName(test.seq, test.idx)
			path := filepath.Join(dir, filename)
			createFileAndClose(t, path)

			w, err := Open(dir, test.at, func(index uint64, data []byte) {})
			if err != test.werr {
				t.Fatalf("want: %v, get: %v", test.werr, err)
			}

			if test.werr != nil {
				return
			}
			defer w.Close()

			if g := filepath.Base(w.back().filename); g != filename {
				t.Errorf("name = %+v, want %+v", g, filename)
			}
		}(i, test)
	}
}

func randRecord() []byte {
	length := rand.Intn(1024 * 10)
	bytes := make([]byte, length)
	for i := 0; i < length; i++ {
		bytes[i] = byte(rand.Intn(256))
	}
	return bytes
}

func TestRestore(t *testing.T) {
	type item struct {
		idx   uint64
		bytes []byte
	}

	totalRecord := 100
	items := make([]item, 0)

	dir := createTmpDir(t)
	defer os.RemoveAll(dir)

	wal, err := Create(dir, 0)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < totalRecord; i++ {
		it := item{
			idx:   uint64(i),
			bytes: randRecord(),
		}
		items = append(items, it)
		ch := wal.Write(it.idx, it.bytes)
		if err = <-ch; err != nil {
			t.Fatal(err)
		}
	}
	wal.Close()

	tests := []uint64{0, 10, 12, 14, 18, 25, 30, 40}
	for _, test := range tests {
		wal, err = Open(dir, test, func(index uint64, data []byte) {
			for i := 0; i < len(items); i++ {
				if items[i].idx == index {
					if !bytes.Equal(items[i].bytes, data) {
						t.Fatalf("restore items wrong")
					}
					return
				}
			}
			t.Fatalf("restore index not found")
		})
		if err != nil {
			t.Fatal(err)
		}
		wal.Close()
	}
}
