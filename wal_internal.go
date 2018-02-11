package wal

import (
	"path/filepath"

	"github.com/thinkermao/wal-go/record"
)

func makeRecordFile(filename string, seq, idx uint64, file *record.File) *recordFile {
	nrf := &recordFile{
		filename:  filename,
		lastIndex: 0,
		seq:       seq,
		index:     idx,
		file:      file,
	}
	return nrf
}

func createFile(dir string, seq, idx uint64) (*recordFile, error) {
	filename := filepath.Join(dir, walName(seq, idx))
	file, err := record.CreateFile(filename)
	if err != nil {
		return nil, err
	}

	nrf := makeRecordFile(filename, seq, idx, file)
	return nrf, nil
}

func closeAll(files []*recordFile) error {
	for _, rf := range files {
		if err := rf.file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (wal *Wal) service(queue <-chan command) {
	for cmd := range queue {
		switch cmd.cmdType {
		case cmdAppend:
			if err := wal.back().file.Write(cmd.index, cmd.data); err != nil {
				cmd.onFailure(err)
			}
			if err := wal.rotateIfNeed(); err != nil {
				cmd.onFailure(err)
			}
			cmd.onSuccess()

		case cmdSync:
			if err := wal.back().file.Sync(); err != nil {
				cmd.onFailure(err)
			} else {
				cmd.onSuccess()
			}
		}
	}
}

func (wal *Wal) rotateIfNeed() error {
	rf := wal.back()
	if !rf.file.Full() {
		return nil
	}

	if err := rf.file.Sync(); err != nil {
		return err
	}

	nrf, err := createFile(wal.walDir, rf.seq+1, rf.lastIndex+1)
	if err != nil {
		return err
	}

	wal.recordFiles = append(wal.recordFiles, nrf)
	return nil
}

func (wal *Wal) back() *recordFile {
	return wal.recordFiles[len(wal.recordFiles)-1]
}
