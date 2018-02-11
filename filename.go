package wal

import (
	"errors"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/thinkermao/wal-go/file"
)

var (
	errBadWalName   = errors.New("bad wal name")
	errFileNotFound = errors.New("file not found")
)

func parseWalName(str string) (seq, index uint64, err error) {
	if !strings.HasSuffix(str, ".wal") {
		return 0, 0, errBadWalName
	}
	_, err = fmt.Sscanf(str, "%016x-%016x.wal", &seq, &index)
	return seq, index, err
}

func mustParseWalName(str string) (uint64, uint64) {
	seq, index, err := parseWalName(str)
	if err != nil {
		log.Panicf("parse correct name should never fail: %v", err)
	}
	return seq, index
}

func walName(seq, index uint64) string {
	return fmt.Sprintf("%016x-%016x.wal", seq, index)
}

func filterWalFiles(names []string) []string {
	result := make([]string, 0)
	for i := 0; i < len(names); i++ {
		if _, _, err := parseWalName(names[i]); err != nil {
			log.Debugf("skip bad wal name: %s", names[i])
			continue
		}
		result = append(result, names[i])
	}
	return result
}

func readAllWalNames(dir string) ([]string, error) {
	names, err := file.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	names = filterWalFiles(names)
	if len(names) == 0 {
		return nil, errFileNotFound
	}
	return names, nil
}

func isValidSequences(names []string) bool {
	var lastSeq uint64
	for _, name := range names {
		curSeq, _ := mustParseWalName(name)
		if lastSeq != 0 && lastSeq != curSeq-1 {
			return false
		}
		lastSeq = curSeq
	}
	return true
}

func searchIndex(names []string, index uint64) (int, bool) {
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		_, curIndex := mustParseWalName(name)
		if index >= curIndex {
			return i, true
		}
	}
	return -1, false
}
