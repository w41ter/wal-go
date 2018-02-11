package wal

import (
	"os"
	"path/filepath"

	"github.com/thinkermao/wal-go/file"
	"github.com/thinkermao/wal-go/record"
)

const defaultSequence = 0

type recordFile struct {
	filename  string
	lastIndex uint64
	seq       uint64
	index     uint64
	file      *record.File
}

// Wal is an implementation of write ahead log.
// It provides the log persistence, recovery capabilities.
// wal is thread-safe and supports concurrent calls.
type Wal struct {
	walDir      string
	recordFiles []*recordFile
	queue       chan<- command
}

// Create returns Wal instance with initialize index.
func Create(walDir string, initialize uint64) (*Wal, error) {
	if !file.IsExists(walDir) {
		if err := os.MkdirAll(walDir, 0766); err != nil {
			return nil, err
		}
	}

	if err := file.ClearAllEndsWith(walDir, ".wal"); err != nil {
		return nil, err
	}

	recordFiles := make([]*recordFile, 0)
	rf, err := createFile(walDir, defaultSequence, initialize)
	if err != nil {
		return nil, err
	}
	recordFiles = append(recordFiles, rf)

	queue := make(chan command)

	wal := &Wal{
		walDir:      walDir,
		recordFiles: recordFiles,
		queue:       queue,
	}
	go wal.service(queue)
	return wal, nil
}

// Open find the first wal file has index large than lsn, and
// read, poll it to consumer.
func Open(walDir string, lsn uint64, consumer record.Consumer) (*Wal, error) {
	// remove all stale tmp files
	if err := file.ClearAllEndsWith(walDir, ".tmp"); err != nil {
		return nil, err
	}
	names, err := readAllWalNames(walDir)
	if err != nil {
		return nil, err
	}

	index, ok := searchIndex(names, lsn)
	if !ok || !isValidSequences(names[index:]) {
		return nil, errFileNotFound
	}

	recordFiles := make([]*recordFile, 0)
	for i := index; i < len(names); i++ {
		path := filepath.Join(walDir, names[i])
		f, err := record.RestoreFile(path, consumer)
		if err != nil {
			closeAll(recordFiles)
			return nil, err
		}
		seq, idx := mustParseWalName(names[i])
		recordFile := makeRecordFile(path, seq, idx, f)
		recordFiles = append(recordFiles, recordFile)
	}

	queue := make(chan command)

	wal := &Wal{
		walDir:      walDir,
		recordFiles: recordFiles,
		queue:       queue,
	}
	go wal.service(queue)
	return wal, nil
}

// Sync used by customer to write buffered data to file.
func (wal *Wal) Sync() <-chan error {
	cmd, ch := genSync()
	wal.queue <- cmd
	return ch
}

// Write store data to buffer.
func (wal *Wal) Write(index uint64, data []byte) <-chan error {
	cmd, ch := genAppend(index, data)
	wal.queue <- cmd
	return ch
}

// Close close working queue, so no any writer could
// append, committed work will be execute. caller must
// ensure no data race at here.
func (wal *Wal) Close() error {
	cmd, errChan := genSync()
	wal.queue <- cmd
	close(wal.queue)

	err := <-errChan
	if err != nil {
		return err
	}

	return closeAll(wal.recordFiles)
}
