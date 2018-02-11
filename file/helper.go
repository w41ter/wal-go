package file

import "strings"
import "os"
import "sort"
import (
	"path/filepath"
)

// ClearAllEndsWith delete all files in the dir directory suffix suf.
func ClearAllEndsWith(dir string, suf string) error {
	names, err := ReadDir(dir)
	if err != nil {
		return err
	}

	for _, name := range names {
		if !strings.HasSuffix(name, suf) {
			continue
		}
		path := filepath.Join(dir, name)
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	return nil
}

// ReadDir returns the filenames in the given directory in sorted order.
func ReadDir(dirPath string) ([]string, error) {
	dir, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

// IsExists reports whether the named file or directory exists.
func IsExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}
