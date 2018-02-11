package wal

import (
	"io/ioutil"
	"os"
	"testing"
)

func BenchmarkWrite100ByteWithoutBatch(b *testing.B) { benchmarkWriteByte(b, 100, 0) }
func BenchmarkWrite100ByteBatch10(b *testing.B)      { benchmarkWriteByte(b, 100, 10) }
func BenchmarkWrite100ByteBatch100(b *testing.B)     { benchmarkWriteByte(b, 100, 100) }
func BenchmarkWrite100ByteBatch500(b *testing.B)     { benchmarkWriteByte(b, 100, 500) }
func BenchmarkWrite100ByteBatch1000(b *testing.B)    { benchmarkWriteByte(b, 100, 1000) }

func BenchmarkWrite1000ByteWithoutBatch(b *testing.B) { benchmarkWriteByte(b, 1000, 0) }
func BenchmarkWrite1000ByteBatch10(b *testing.B)      { benchmarkWriteByte(b, 1000, 10) }
func BenchmarkWrite1000ByteBatch100(b *testing.B)     { benchmarkWriteByte(b, 1000, 100) }
func BenchmarkWrite1000ByteBatch500(b *testing.B)     { benchmarkWriteByte(b, 1000, 500) }
func BenchmarkWrite1000ByteBatch1000(b *testing.B)    { benchmarkWriteByte(b, 1000, 1000) }

func benchmarkWriteByte(b *testing.B, size int, batch int) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(p)

	w, err := Create(p, 0)
	if err != nil {
		b.Fatal(err)
	}
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte(i)
	}
	b.ResetTimer()
	n := 0
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		ch := w.Write(0, data)
		err = <-ch
		if err != nil {
			b.Fatal(err)
		}
		n++
		if n > batch {
			ch := w.Sync()
			err = <-ch
			if err != nil {
				b.Fatal(err)
			}
			n = 0
		}
	}
	ch := w.Sync()
	err = <-ch
	if err != nil {
		b.Fatal(err)
	}
	w.Close()
}
