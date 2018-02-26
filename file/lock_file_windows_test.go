package file

import (
	"os"
	"testing"
)

func TestLockFile_Lock(t *testing.T) {
	file, err := OpenFile("./xxx", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Error(err)
	}

	file2, err := OpenFile("./xxx", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Error(err)
	}

	err = file.Lock()
	if err != nil {
		t.Error(err)
	}

	err = file2.Lock()
	if err == nil {
		t.Errorf("lock ./xxx must failed")
	}

	err = file.Unlock()
	if err != nil {
		t.Error(err)
	}

	file.Close()
	file2.Close()

	os.Remove("./xxx")
}

func TestLockFile_Unlock(t *testing.T) {
	file, err := OpenFile("./xxxx", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Error(err)
	}

	file2, err := OpenFile("./xxxx", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Error(err)
	}

	err = file.Lock()
	if err != nil {
		t.Error(err)
	}

	err = file.Unlock()
	if err != nil {
		t.Error(err)
	}

	err = file2.Lock()
	if err != nil {
		t.Error(err)
	}

	err = file2.Unlock()
	if err != nil {
		t.Error(err)
	}

	file.Close()
	file2.Close()

	os.Remove("./xxxx")
}
