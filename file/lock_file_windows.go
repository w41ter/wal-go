package file

import (
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

// Unlock will relase lock this file holded.
func (l *LockFile) Unlock() error {
	h, err := syscall.LoadLibrary("kernel32.dll")
	if err != nil {
		return err
	}
	defer syscall.FreeLibrary(h)

	addr, err := syscall.GetProcAddress(h, "UnlockFile")
	if err != nil {
		return err
	}
	r0, _, err := syscall.Syscall6(addr, 5, l.Fd(), 0, 0, 0, 1, 0)
	if 0 == int(r0) {
		return err
	}
	return nil
}

// Lock will lock this file, if lock failed, err not nil.
func (l *LockFile) Lock() error {
	h, err := syscall.LoadLibrary("kernel32.dll")
	if err != nil {
		return err
	}
	defer syscall.FreeLibrary(h)

	addr, err := syscall.GetProcAddress(h, "LockFile")
	if err != nil {
		return err
	}
	r0, _, err := syscall.Syscall6(addr, 5, l.Fd(), 0, 0, 0, 1, 0)
	if 0 != int(r0) {
		return nil
	}
	return err
}
