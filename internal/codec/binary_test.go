package codec

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// TestEncodeDecodeRoundTrip verifies that normal records and tombstones
// survive serialization and deserialization with exact byte matches.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		name        string
		key         []byte
		val         []byte
		isTombstone bool
	}{
		{
			name:        "standard record",
			key:         []byte("user:1001"),
			val:         []byte(`{"name":"alice","role":"admin"}`),
			isTombstone: false,
		},
		{
			name:        "empty key and value",
			key:         []byte(""),
			val:         []byte(""),
			isTombstone: false,
		},
		{
			name:        "binary key and arbitrary byte payload",
			key:         []byte{0x00, 0xFF, 0x1A, 0x7E},
			val:         []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01},
			isTombstone: false,
		},
		{
			name:        "tombstone with empty value",
			key:         []byte("user:1001"),
			val:         []byte(""),
			isTombstone: true,
		},
		{
			name:        "tombstone ignores passed value bytes",
			key:         []byte("deleted_key"),
			val:         []byte("this-should-not-persist"),
			isTombstone: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			n, err := EncodeEntry(&buf, tt.key, tt.val, tt.isTombstone)
			if err != nil {
				t.Fatalf("unexpected encode error: %v", err)
			}
			if n != buf.Len() {
				t.Fatalf("returned byte count %d does not match buffer length %d", n, buf.Len())
			}

			decoded, err := DecodeEntry(&buf)
			if err != nil {
				t.Fatalf("unexpected decode error: %v", err)
			}

			if !bytes.Equal(decoded.Key, tt.key) {
				t.Errorf("key mismatch: got %q, want %q", decoded.Key, tt.key)
			}

			if tt.isTombstone {
				if !decoded.IsTombstone {
					t.Errorf("expected tombstone flag to be true")
				}
				if len(decoded.Value) != 0 {
					t.Errorf("tombstone value length should be 0, got %d", len(decoded.Value))
				}
			} else {
				if decoded.IsTombstone {
					t.Errorf("expected tombstone flag to be false")
				}
				if !bytes.Equal(decoded.Value, tt.val) {
					t.Errorf("val mismatch: got %q, want %q", decoded.Value, tt.val)
				}
			}
		})
	}
}

// TestCorruptedChecksum verifies that single-bit mutations in header, payload,
// or checksum trigger ErrCorruptData.
func TestCorruptedChecksum(t *testing.T) {
	key := []byte("session_token")
	val := []byte("valid_token_data_hash")

	var buf bytes.Buffer
	_, err := EncodeEntry(&buf, key, val, false)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	rawBytes := buf.Bytes()

	corruptionOffsets := []struct {
		name   string
		offset int
	}{
		{"corrupt header keyLen", 0},
		{"corrupt header flag", 6},
		{"corrupt key byte", 8},
		{"corrupt value byte", HeaderSize + len(key) + 2},
		{"corrupt checksum byte", len(rawBytes) - 1},
	}

	for _, tc := range corruptionOffsets {
		t.Run(tc.name, func(t *testing.T) {
			corrupted := make([]byte, len(rawBytes))
			copy(corrupted, rawBytes)

			// Flip bits
			corrupted[tc.offset] ^= 0xFF

			r := bytes.NewReader(corrupted)
			_, err := DecodeEntry(r)
			if err == nil {
				t.Fatalf("expected error due to corrupted data, got nil")
			}

			// A mutated length field can cause a read past the end of stream (io.ErrUnexpectedEOF)
			// before reaching the checksum comparison. Both are valid corruption signals.
			if !errors.Is(err, ErrCorruptData) && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("expected ErrCorruptData or ErrUnexpectedEOF, got: %v", err)
			}
		})
	}
}

// TestKeyTooLarge verifies uint16 overflow checks.
func TestKeyTooLarge(t *testing.T) {
	oversizedKey := make([]byte, 0x10000) // 65536 bytes
	var buf bytes.Buffer

	_, err := EncodeEntry(&buf, oversizedKey, []byte("val"), false)
	if !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("expected ErrKeyTooLarge, got: %v", err)
	}
}

// TestTruncatedStreams verifies graceful handling of truncated records (partial writes / crashes).
func TestTruncatedStreams(t *testing.T) {
	var buf bytes.Buffer
	_, err := EncodeEntry(&buf, []byte("mykey"), []byte("myval"), false)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	encoded := buf.Bytes()

	// Truncate at multiple stages: partial header, partial payload, partial CRC
	truncations := []int{
		HeaderSize - 2,
		HeaderSize + 2,
		len(encoded) - 2,
	}

	for _, cut := range truncations {
		r := bytes.NewReader(encoded[:cut])
		_, err := DecodeEntry(r)
		if !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
			t.Errorf("expected EOF/UnexpectedEOF for cut length %d, got: %v", cut, err)
		}
	}
}

// TestStreamDecodingMultipleEntries tests decoding back-to-back entries from a stream (SSTable/WAL scan).
func TestStreamDecodingMultipleEntries(t *testing.T) {
	var stream bytes.Buffer

	records := []struct {
		k []byte
		v []byte
		t bool
	}{
		{[]byte("k1"), []byte("v1"), false},
		{[]byte("k2"), nil, true},
		{[]byte("k3"), []byte("v3_long_value"), false},
	}

	for _, r := range records {
		if _, err := EncodeEntry(&stream, r.k, r.v, r.t); err != nil {
			t.Fatalf("stream write failed: %v", err)
		}
	}

	reader := bytes.NewReader(stream.Bytes())
	for i, want := range records {
		entry, err := DecodeEntry(reader)
		if err != nil {
			t.Fatalf("failed decoding record %d: %v", i, err)
		}
		if !bytes.Equal(entry.Key, want.k) {
			t.Errorf("record %d key mismatch: got %s, want %s", i, entry.Key, want.k)
		}
		if entry.IsTombstone != want.t {
			t.Errorf("record %d tombstone mismatch: got %v, want %v", i, entry.IsTombstone, want.t)
		}
	}

	// Stream should now be exhausted
	_, err := DecodeEntry(reader)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF at end of stream, got %v", err)
	}
}