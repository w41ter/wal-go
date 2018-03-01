package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync/atomic"

	"github.com/thinkermao/wal-go/file"
	"github.com/thinkermao/wal-go/utils/pd"
)

const (
	recordFileSize = 1024 * 1024 * 64 // Record file default size.
)

var (
	crc32Table = crc32.MakeTable(crc32.Koopman) // checksum use crc32

	// errors returns when something bad.
	errEmptyRecord   = errors.New("write empty record")
	errUnexpectedEOF = errors.New("unexpected end of file")
	errBadChecksum   = errors.New("bad checksum")
)

// Consumer used by RestoreFile, to consume restored records.
type Consumer func(index uint64, data []byte) error

// File is record file, it has buffer, and preallocated
// recordFileSize whitespace when first create it.
type File struct {
	filename string
	file     *file.LockFile
	buffer   *file.Buffer
	size     uint32
	offset   uint32
}

// RestoreFile open record file and restore records, push to consumer.
func RestoreFile(filename string, at uint64, consumer Consumer) (*File, error) {
	record, err := CreateFile(filename)
	if err != nil {
		return nil, err
	}

	var off uint32
	if off, err = readAllRecords(record.buffer, at, consumer); err != nil {
		return nil, err
	}
	record.offset = off

	return record, nil
}

// CreateFile create record file with given filename.
func CreateFile(filename string) (*File, error) {
	fd, err := file.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}

	if err = fd.Truncate(recordFileSize); err != nil {
		fd.Close()
		return nil, err
	}

	if err = fd.Lock(); err != nil {
		fd.Close()
		return nil, err
	}

	buffer := file.BufferCreate(fd)

	record := &File{
		filename: filename,
		file:     fd,
		buffer:   buffer,
		size:     recordFileSize,
		offset:   0,
	}

	return record, nil
}

// Close unlock and close current file,
// if current file not sync, call sync.
func (rf *File) Close() error {
	if err := rf.Sync(); err != nil {
		return err
	}

	if err := rf.file.Unlock(); err != nil {
		return err
	}

	return rf.file.Close()
}

// Full test whether current file size great than rotate size.
func (rf *File) Full() bool {
	return atomic.LoadUint32(&rf.offset) >= rf.size
}

func (rf *File) Write(index uint64, data []byte) error {
	if len(data) == 0 {
		return errEmptyRecord
	}

	recrd := record{
		Index: index,
		Data:  data,
	}

	recrd.Crc32 = getCrc32(&recrd)
	bytes, err := pd.Marshal(&recrd)
	if err != nil {
		return err
	}

	length := uint32(len(bytes))
	buf := [4]byte{}
	binary.LittleEndian.PutUint32(buf[:], length)
	if err := rf.buffer.Write(buf[:]); err != nil {
		return err
	}

	if err := rf.buffer.Write(bytes); err != nil {
		return err
	}

	atomic.AddUint32(&rf.offset, length+4)

	return nil
}

// Sync flush buffer, ans sync file.
func (rf *File) Sync() error {
	if err := rf.buffer.Flush(); err != nil {
		return err
	}

	return rf.file.Sync()
}

func readAllRecords(reader *file.Buffer, at uint64, consumer Consumer) (uint32, error) {
	var eat uint32
	for {
		length, recrd, err := readRecord(reader)
		if err != nil {
			if err == io.EOF {
				return 0, errUnexpectedEOF
			}
			return 0, err
		}

		if length == 0 {
			// end
			break
		}

		wchecksum := getCrc32(&recrd)
		if wchecksum != recrd.Crc32 {
			return 0, errBadChecksum
		}

		if recrd.Index >= at {
			if err := consumer(recrd.Index, recrd.Data); err != nil {
				return 0, err
			}
		}
		eat += length + 4
	}
	return eat, nil
}

func readRecord(reader *file.Buffer) (length uint32, record record, err error) {
	uint32buf := [4]byte{}
	err = reader.Read(uint32buf[:])
	if err == io.EOF {
		err = nil
		return
	}

	length = binary.LittleEndian.Uint32(uint32buf[:])
	if length == 0 {
		return
	}

	bytes := make([]byte, length)
	if err = reader.Read(bytes); err != nil {
		return
	}

	err = pd.Unmarshal(&record, bytes)

	return
}

func getCrc32(record *record) uint32 {
	a := [8]byte{}
	binary.LittleEndian.PutUint64(a[:], record.Index)
	crc := crc32.Checksum(a[:], crc32Table)
	crc = crc32.Update(crc, crc32Table, record.Data)
	return crc
}
