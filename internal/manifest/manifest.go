package manifest

import (
	"os"
	"encoding/gob"
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

    var manifest Manifest
    decoder := gob.NewDecoder(file)
    err = decoder.Decode(&manifest)
    if err != nil {
        return nil, err
    }

    return &manifest, nil
}

func WriteManifest(path string, manifest *Manifest) error {
    tmpPath := path + ".tmp"
    file, err := os.Create(tmpPath)
    if err != nil {
        return err
    }

    encoder := gob.NewEncoder(file)
    if err := encoder.Encode(manifest); err != nil {
        file.Close()
        os.Remove(tmpPath)
        return err
    }

    if err := file.Close(); err != nil {
        return err
    }
    
    return os.Rename(tmpPath, path)
}