package codec

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

const (
	HeaderSize    = 7 // 2B (KeyLen) + 4B (ValLen) + 1B (Flag)
	FlagNormal    = byte(0x00)
	FlagTombstone = byte(0x01)
)

var (
	ErrCorruptData = errors.New("checksum mismatch: corrupt record")
	ErrKeyTooLarge = errors.New("key length exceeds uint16 limit")
)

type Entry struct {
	Key         []byte
	Value       []byte
	IsTombstone bool
}

func EncodeEntry(w io.Writer, key, val []byte, isTombstone bool) (int, error) {
	if len(key) > 0xFFFF {
		return 0, ErrKeyTooLarge
	}

	flag := FlagNormal
	valLen := uint32(len(val))
	if isTombstone {
		flag = FlagTombstone
		valLen = 0
	}

	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint16(header[0:2], uint16(len(key)))
	binary.BigEndian.PutUint32(header[2:6], valLen)
	header[6] = flag

	crc := crc32.NewIEEE()
	crc.Write(header)
	crc.Write(key)
	if !isTombstone {
		crc.Write(val)
	}
	checksum := crc.Sum32()

	checksumBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(checksumBuf, checksum)

	// Sequential writes to target io.Writer
	totalWritten := 0
	parts := [][]byte{header, key}
	if !isTombstone {
		parts = append(parts, val)
	}
	parts = append(parts, checksumBuf)

	for _, b := range parts {
		n, err := w.Write(b)
		totalWritten += n
		if err != nil {
			return totalWritten, err
		}
	}

	return totalWritten, nil
}

func DecodeEntry(r io.Reader) (*Entry, error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	keyLen := binary.BigEndian.Uint16(header[0:2])
	valLen := binary.BigEndian.Uint32(header[2:6])
	flag := header[6]

	payload := make([]byte, int(keyLen)+int(valLen))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	var checksumBuf [4]byte
	if _, err := io.ReadFull(r, checksumBuf[:]); err != nil {
		return nil, err
	}
	expectedChecksum := binary.BigEndian.Uint32(checksumBuf[:])

	crc := crc32.NewIEEE()
	crc.Write(header)
	crc.Write(payload)
	if crc.Sum32() != expectedChecksum {
		return nil, ErrCorruptData
	}

	return &Entry{
		Key:         payload[:keyLen],
		Value:       payload[keyLen:],
		IsTombstone: flag == FlagTombstone,
	}, nil
}