package manifest

import (
	"os"
	"io"
    "encoding/binary"
    "errors"
)

var (
	ErrCorruptManifest = errors.New("corrupt manifest: unexpected EOF or invalid length")
	ErrPathTooLong     = errors.New("manifest path exceeds uint16 max length")
)

type Manifest struct {
    SSTablePaths []string
}

func ReadManifest(path string) (*Manifest, error) {
    file, err := os.Open(path)
    if err != nil {
        if os.IsNotExist(err) {
            // If manifest doesn't exist, return empty manifest
            return &Manifest{SSTablePaths: []string{}}, nil
        }
        return nil, err
    }
    defer file.Close()

    var countBuf [4]byte
	if _, err := io.ReadFull(file, countBuf[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return &Manifest{SSTablePaths: []string{}}, nil
		}
		return nil, err
	}
	count := binary.BigEndian.Uint32(countBuf[:])
    paths := make([]string, 0, count)

    var lenBuf [2]byte
	for i := uint32(0); i < count; i++ {
		if _, err := io.ReadFull(file, lenBuf[:]); err != nil {
			return nil, ErrCorruptManifest
		}
		strLen := binary.BigEndian.Uint16(lenBuf[:])

		strBytes := make([]byte, strLen)
		if _, err := io.ReadFull(file, strBytes); err != nil {
			return nil, ErrCorruptManifest
		}

		paths = append(paths, string(strBytes))
	}

	return &Manifest{SSTablePaths: paths}, nil
}

func WriteManifest(path string, manifest *Manifest) error {
    tmpPath := path + ".tmp"
    file, err := os.Create(tmpPath)
    if err != nil {
        return err
    }

    count := uint32(len(manifest.SSTablePaths))
	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], count)

	if _, err := file.Write(countBuf[:]); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}

    var lenBuf [2]byte
	for _, p := range manifest.SSTablePaths {
		if len(p) > 0xFFFF {
			file.Close()
			os.Remove(tmpPath)
			return ErrPathTooLong
		}

		binary.BigEndian.PutUint16(lenBuf[:], uint16(len(p)))

		if _, err := file.Write(lenBuf[:]); err != nil {
			file.Close()
			os.Remove(tmpPath)
			return err
		}

		if _, err := file.Write([]byte(p)); err != nil {
			file.Close()
			os.Remove(tmpPath)
			return err
		}
	}

    if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := file.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
    
    return os.Rename(tmpPath, path)
}