package manifest

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestManifestNonExistentFile verifies that attempting to read a manifest
// from a path that does not exist returns an empty manifest and nil error.
func TestManifestNonExistentFile(t *testing.T) {
	dir := t.TempDir()
	nonExistentPath := filepath.Join(dir, "MANIFEST")

	m, err := ReadManifest(nonExistentPath)
	if err != nil {
		t.Fatalf("expected nil error on missing manifest, got: %v", err)
	}

	if len(m.SSTablePaths) != 0 {
		t.Fatalf("expected 0 SSTablePaths, got %d", len(m.SSTablePaths))
	}
}

// TestManifestEmptyFile verifies that an existing but 0-byte manifest file
// returns an empty manifest and nil error.
func TestManifestEmptyFile(t *testing.T) {
	dir := t.TempDir()
	emptyPath := filepath.Join(dir, "MANIFEST")

	if err := os.WriteFile(emptyPath, []byte{}, 0644); err != nil {
		t.Fatalf("failed to create empty file: %v", err)
	}

	m, err := ReadManifest(emptyPath)
	if err != nil {
		t.Fatalf("expected nil error on 0-byte manifest, got: %v", err)
	}

	if len(m.SSTablePaths) != 0 {
		t.Fatalf("expected 0 SSTablePaths, got %d", len(m.SSTablePaths))
	}
}

// TestManifestWriteAndReadRoundTrip tests writing and reading multiple SSTable paths,
// preserving exact order and checking that temporary files (.tmp) are cleaned up.
func TestManifestWriteAndReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")

	expectedPaths := []string{
		"data/sstable_00001.sst",
		"data/sstable_00002.sst",
		"data/nested/dir/sstable_00003.sst",
		"single.sst",
	}

	initialManifest := &Manifest{
		SSTablePaths: expectedPaths,
	}

	if err := WriteManifest(manifestPath, initialManifest); err != nil {
		t.Fatalf("WriteManifest failed: %v", err)
	}

	// Verify atomic write: .tmp file should not linger
	tmpPath := manifestPath + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("temporary file %s still exists after successful WriteManifest", tmpPath)
	}

	// Read and verify round trip
	readManifest, err := ReadManifest(manifestPath)
	if err != nil {
		t.Fatalf("ReadManifest failed: %v", err)
	}

	if len(readManifest.SSTablePaths) != len(expectedPaths) {
		t.Fatalf("expected %d paths, got %d", len(expectedPaths), len(readManifest.SSTablePaths))
	}

	for i, want := range expectedPaths {
		if got := readManifest.SSTablePaths[i]; got != want {
			t.Errorf("path mismatch at index %d: got %q, want %q", i, got, want)
		}
	}
}

// TestManifestPathTooLong asserts that paths exceeding uint16 max length (65535 bytes)
// are rejected with ErrPathTooLong without creating/persisting the target manifest.
func TestManifestPathTooLong(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "MANIFEST")

	oversizedPath := strings.Repeat("a", 0x10000) // 65536 bytes
	manifest := &Manifest{
		SSTablePaths: []string{oversizedPath},
	}

	err := WriteManifest(manifestPath, manifest)
	if !errors.Is(err, ErrPathTooLong) {
		t.Fatalf("expected ErrPathTooLong, got: %v", err)
	}

	// Target manifest and temp file should not exist
	if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
		t.Errorf("manifest file should not exist after failed validation")
	}
	if _, err := os.Stat(manifestPath + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("temporary file should be cleaned up after error")
	}
}

// TestManifestCorruptFile ensures truncated data or bad string lengths
// return ErrCorruptManifest instead of panicking.
func TestManifestCorruptFile(t *testing.T) {
	dir := t.TempDir()

	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "truncated path length header",
			// count = 1, but truncated mid-uint16 string length
			data: []byte{0x00, 0x00, 0x00, 0x01, 0x00},
		},
		{
			name: "length claims 10 bytes but stream terminates early",
			// count = 1, strLen = 10 (0x00, 0x0A), only 4 bytes of path payload
			data: []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x0A, 't', 'e', 's', 't'},
		},
		{
			name: "count claims 3 entries but file contains only 1",
			// count = 3, only 1 valid entry: strLen = 4, "test"
			data: []byte{0x00, 0x00, 0x00, 0x03, 0x00, 0x04, 't', 'e', 's', 't'},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(dir, tt.name+".manifest")
			if err := os.WriteFile(path, tt.data, 0644); err != nil {
				t.Fatalf("failed to write corrupt manifest file: %v", err)
			}

			_, err := ReadManifest(path)
			if !errors.Is(err, ErrCorruptManifest) {
				t.Fatalf("expected ErrCorruptManifest, got: %v", err)
			}
		})
	}
}

// TestManifestAtomic