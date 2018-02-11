package wal

type cmdType int

const (
	cmdSync cmdType = iota
	cmdAppend
)

type command struct {
	cmdType cmdType
	result  chan error
	index   uint64
	data    []byte
}

func genSync() (command, <-chan error) {
	result := make(chan error, 1)
	return command{
		cmdType: cmdSync,
		result:  result,
	}, result
}

func genAppend(index uint64, bytes []byte) (command, <-chan error) {
	result := make(chan error, 1)
	return command{
		cmdType: cmdAppend,
		data:    bytes,
		index:   index,
		result:  result,
	}, result
}

func (wc *command) onSuccess() {
	wc.result <- nil
	close(wc.result)
}

func (wc *command) onFailure(err error) {
	wc.result <- err
	close(wc.result)
}
