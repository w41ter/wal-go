package file

import (
	"fmt"
	"os"
	"syscall"
)

// LockFile extends os.File to provide it with the ability to mutexes.
type LockFile struct {
	*os.File
}

// OpenFile same as os.OpenFile, but returns LockFile.
func OpenFile(name string, flag int, perm os.FileMode) (*LockFile, error) {
	var file LockFile
	var err error
	file.File, err = os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	return &file, err
}

// Lock will lock this file, if lock failed, err not nil.
func (l *LockFile) Lock() error {
	err := syscall.Flock(int(l.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return fmt.Errorf("cannot flock directory %s - %s", l.Name(), err)
	}
	return nil
}

// Unlock will relase lock this file holded.
func (l *LockFile) Unlock() error {
	return syscall.Flock(int(l.Fd()), syscall.LOCK_UN)
}
