package encoding

import (
	"encoding/binary"
	"io"
)

type byteReaderFunc func() (byte, error)

func (fn byteReaderFunc) ReadByte() (byte, error) {
	return fn()
}

func putUint32(v uint32) (buf [4]byte) {
	binary.LittleEndian.PutUint32(buf[:], v)
	return
}

func putVarInt(v int64) (buf [8]byte, n int) {
	n = binary.PutVarint(buf[:], v)
	return
}

func readUint32(buffer []byte) (uint32, error) {
	if len(buffer) < 4 {
		return 0, io.ErrShortBuffer
	}
	return binary.LittleEndian.Uint32(buffer[0:4]), nil
}

func insertAt(buffer []byte, pos int, bytes ...byte) []byte {
	target := shift(buffer, pos, len(bytes))
	copy(target[pos:], bytes)
	return target
}

func shift(buffer []byte, pos, length int) []byte {
	target := buffer
	if want := len(target) + length; cap(target) < want {
		size := cap(target) * 2
		if want > size {
			size = want * 3 / 2
		}
		target = make([]byte, 0, size)
		target = append(target, buffer...)
	}

	var (
		remain  = len(buffer) - pos
		outside = max(length-remain, 0)
	)

	// append the parts of bytes that extend beyond the current boundary of the buffer
	for i := outside; i > 0; i-- {
		target = append(target, 0)
	}

	// append the parts of the existing buffer that will extend beyond current boundary
	if n := length - outside; n > 0 {
		target = append(target, buffer[len(buffer)-n:]...)
	}

	// internally shift all characters from pos to the end
	for i := len(buffer) - 1; i-length >= pos; i-- {
		target[i] = target[i-length]
	}

	return target
}

func unshift(buffer []byte, pos, length int) []byte {
	copy(buffer[pos:], buffer[pos+length:])
	return buffer[0 : len(buffer)-length]
}

func max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}
