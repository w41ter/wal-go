package pd

import (
	"bytes"
	"encoding/gob"

	log "github.com/sirupsen/logrus"
)

// Messager used by Marshal/Unmashal
type Messager interface {
	Reset()
}

// Marshal encoding msg by gob.
func Marshal(msg Messager) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(msg); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MustMarshal same as Marshal, but panic when
// error ocur in marshaling.
func MustMarshal(msg Messager) []byte {
	d, err := Marshal(msg)
	if err != nil {
		log.Panicf("marshal should never fail (%v)", err)
	}
	return d
}

// Unmarshal data to msg by gob.
func Unmarshal(msg Messager, data []byte) error {
	buf := bytes.NewBuffer(data)
	decode := gob.NewDecoder(buf)
	return decode.Decode(msg)
}

// MustUnmarshal same as Unmarshal, but panic
// when unmarshal failed.
func MustUnmarshal(msg Messager, data []byte) {
	if err := Unmarshal(msg, data); err != nil {
		log.Panicf("unmarshal should never fail (%v)", err)
	}
}

// MaybeUnmarshal instead of panic, just return true or
// false whether error ocurr.
func MaybeUnmarshal(msg Messager, data []byte) bool {
	return Unmarshal(msg, data) == nil
}
