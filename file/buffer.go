package file

import (
	"io"
)

const bufferSize = 4 * 1024 // default buffer size 4K

// Buffer give buffer io support for io.ReadWriter.
// 	file, _ := os.Create(filename)
// 	buffer := file.BufferCreate(file)
// 	bytes := make([]byte, n)
// 	for {
//		buffer.WRite(buffer)
// 		...
// 	}
// 	buffer.Flush()
// or
//  buffer.Read(buffer)
type Buffer struct {
	file         io.ReadWriter
	inboundMark  int
	outboundMark int
	outboundSize int
	inbound      []byte
	outbound     []byte
}

// BufferCreate buffered exists io.ReadWriter.
// Notice: buffer will no flush when exit, caller must ensure flush it.
func BufferCreate(file io.ReadWriter) *Buffer {
	return &Buffer{
		file:         file,
		inbound:      make([]byte, bufferSize),
		outbound:     make([]byte, bufferSize),
		inboundMark:  0,
		outboundMark: 0,
		outboundSize: 0,
	}
}

// Write append data to buffer, wait for flush to file.
func (buf *Buffer) Write(bytes []byte) error {
	var err error
	for err == nil && len(bytes) > 0 {
		if buf.inboundMark != 0 || len(bytes) < bufferSize {
			wtn := buf.fillInbound(bytes)
			bytes = bytes[wtn:]
		} else {
			return buf.writeDirectly(bytes)
		}

		if buf.isInboundFull() {
			err = buf.Flush()
		}
	}
	return err
}

// Flush write buffered data to file, there will
// be no impact if no buffered data.
func (buf *Buffer) Flush() error {
	if buf.inboundMark == 0 {
		return nil
	}

	bytes := buf.inbound
	if !buf.isInboundFull() {
		bytes = bytes[:buf.inboundMark]
	}

	if _, err := buf.file.Write(bytes); err != nil {
		return err
	}
	buf.inboundMark = 0

	return nil
}

// Readn reads up to length bytes from the File.
// It returns any error encountered. At end of file, Read returns io.EOF.
func (buf *Buffer) Readn(bytes []byte, length int) error {
	// push any data to io, ensure consistency.
	buf.Flush()

	var err error
	for err == nil && length > 0 {
		if !buf.isOutboundEmpty() {
			rdn := buf.releaseOutbound(bytes, length)
			bytes = bytes[rdn:]
			length -= rdn
		} else if length >= bufferSize {
			return buf.readDirectly(bytes, length)
		} else if buf.isOutboundEmpty() {
			err = buf.loadOutbound()
		}
	}
	return err
}

// Read like Readn.
func (buf *Buffer) Read(bytes []byte) error {
	return buf.Readn(bytes, len(bytes))
}

func (buf *Buffer) inboundLeft() int {
	return bufferSize - buf.inboundMark
}

func (buf *Buffer) isInboundFull() bool {
	return bufferSize == buf.inboundMark
}

func (buf *Buffer) outboundLeft() int {
	return buf.outboundSize - buf.outboundMark
}

func (buf *Buffer) isOutboundEmpty() bool {
	return buf.outboundMark == buf.outboundSize
}

//
// writeDirectly Write bytes to file without buf,
// it must be only called by Write, rather than user.
func (buf *Buffer) writeDirectly(bytes []byte) error {
	_, err := buf.file.Write(bytes)
	return err
}

func (buf *Buffer) readDirectly(bytes []byte, length int) error {
	_, err := buf.file.Read(bytes[:length])
	return err
}

//
// fillInbound fill inbound with bytes, and return
// size of filled elements.
func (buf *Buffer) fillInbound(bytes []byte) int {
	end := min(buf.inboundLeft(), len(bytes))
	copy(buf.inbound[buf.inboundMark:], bytes[:end])
	buf.inboundMark += end
	return end
}

func (buf *Buffer) releaseOutbound(bytes []byte, length int) int {
	end := min(buf.outboundLeft(), length)
	outbound := buf.outbound[buf.outboundMark:]
	copy(bytes, outbound[:end])
	buf.outboundMark += end
	return end
}

func (buf *Buffer) loadOutbound() error {
	n, err := buf.file.Read(buf.outbound)
	buf.outboundSize = n
	buf.outboundMark = 0
	return err
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}
