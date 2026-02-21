// Package offset provides persistent offset storage for tracking ServiceNow
// table polling progress.
//
// # What Are Offsets?
//
// An offset records the position of the last successfully processed record for
// a given ServiceNow table. This allows the source poller to resume exactly
// where it left off after a restart, without re-processing records.
//
// Each offset consists of two fields:
//
//   - Timestamp: The sys_updated_on (or equivalent) value of the last record.
//     Stored as Unix epoch seconds, matching the Java reference.
//
//   - LastIdentifier: The sys_id (or equivalent) of the last record processed
//     at that timestamp. Required because multiple records may share the same
//     timestamp — the identifier breaks the tie and prevents duplicates.
//
// This two-field design is a direct port of the Java TimestampSourceOffset.java
// from the IBM kafka-connect-servicenow connector.
//
// # Ordering Guarantee
//
// The poller queries ServiceNow with ORDER BY sys_updated_on ASC, sys_id ASC.
// The bounded query uses two union clauses:
//
//  1. Records where timestamp = lastTimestamp AND sys_id > lastIdentifier
//  2. Records where timestamp BETWEEN (lastTimestamp, throughTimestamp) exclusive
//
// This ensures at-least-once delivery: each record is seen exactly once under
// normal operation, and at-most duplicated (never lost) on crash recovery.
//
// # Storage Backends
//
// Two storage backends are provided:
//
//   - [FileStore]: Writes offsets to a JSON file on disk. Suitable for
//     single-instance deployments. Uses atomic write (write to temp → rename)
//     to prevent corruption.
//
//   - [KafkaStore]: Writes offsets to a Kafka compacted topic. Suitable for
//     distributed deployments where the bridge may run on different hosts.
//     (Planned for Phase 5 — currently only the interface and file store are
//     provided.)
//
// # Thread Safety
//
// All Store implementations and the Offset type methods are safe for concurrent use.
package offset

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Offset represents the progress marker for a single ServiceNow table.
// It tracks the last seen timestamp and the last processed record identifier,
// allowing the poller to construct bounded queries that resume exactly where
// the previous poll cycle left off.
//
// This is the Go equivalent of TimestampSourceOffset.java from the Java reference.
type Offset struct {
	// Timestamp is the sys_updated_on value of the last processed record,
	// stored as Unix epoch seconds. This matches the Java reference which
	// stores Instant.getEpochSecond().
	Timestamp int64 `json:"timestamp"`

	// LastIdentifier is the sys_id (or configured identifier field) of the
	// last processed record at this timestamp. Used to break ties when
	// multiple records share the same timestamp.
	LastIdentifier string `json:"last_identifier"`
}

// Time returns the Timestamp as a time.Time in UTC.
func (o Offset) Time() time.Time {
	return time.Unix(o.Timestamp, 0).UTC()
}

// IsZero returns true if the offset has not been initialized.
func (o Offset) IsZero() bool {
	return o.Timestamp == 0 && o.LastIdentifier == ""
}

// Store defines the interface for offset persistence. Implementations must
// be safe for concurrent use by multiple goroutines (one per table poller).
type Store interface {
	// Get retrieves the stored offset for the given table. Returns an empty
	// Offset (IsZero() == true) if no offset has been stored for this table.
	Get(table string) (Offset, error)

	// Set stores the offset for the given table. This should be called only
	// after the corresponding records have been successfully produced to Kafka.
	Set(table string, offset Offset) error

	// Flush persists any buffered changes to the backing storage. For the
	// FileStore this writes to disk; for KafkaStore this produces to Kafka.
	Flush() error

	// Close flushes and releases any resources.
	Close() error
}

// ----- File-Based Offset Store -----

// FileStore persists offsets as a JSON file on the local filesystem.
//
// # File Format
//
// The file contains a single JSON object mapping table names to their offsets:
//
//	{
//	  "incident": {"timestamp": 1706140800, "last_identifier": "abc123"},
//	  "change_request": {"timestamp": 1706137200, "last_identifier": "def456"}
//	}
//
// # Durability Guarantees
//
// FileStore uses atomic writes to prevent file corruption during a crash:
//
//  1. Write the new state to a temporary file in the same directory.
//  2. Sync the temporary file to ensure data reaches disk.
//  3. Rename the temporary file to the target file (atomic on POSIX systems).
//
// This guarantees that the offset file is always in a valid state, even if the
// process is killed mid-write. The worst case is losing the most recent update,
// which maps to at-most one extra poll cycle (at-least-once semantics).
//
// # Concurrency
//
// FileStore is safe for concurrent use. Internal state is protected by a
// sync.RWMutex, allowing concurrent Gets from multiple poller goroutines
// while serializing Sets and Flushes.
type FileStore struct {
	path    string
	mu      sync.RWMutex
	offsets map[string]Offset
	dirty   bool
}

// NewFileStore creates a FileStore that persists offsets to the given file path.
// If the file already exists, its contents are loaded into memory. If the file
// does not exist, the store starts with an empty map.
func NewFileStore(path string) (*FileStore, error) {
	fs := &FileStore{
		path:    path,
		offsets: make(map[string]Offset),
	}

	// Try to load existing offsets from disk.
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// No existing file — start fresh.
			return fs, nil
		}
		return nil, fmt.Errorf("reading offset file %s: %w", path, err)
	}

	if len(data) > 0 {
		if err := json.Unmarshal(data, &fs.offsets); err != nil {
			return nil, fmt.Errorf("parsing offset file %s: %w", path, err)
		}
	}

	return fs, nil
}

// Get retrieves the offset for the given table. Returns an empty Offset if
// the table has not been polled before (IsZero() == true).
func (fs *FileStore) Get(table string) (Offset, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.offsets[table], nil
}

// Set stores the offset for the given table in memory. The change is not
// persisted to disk until Flush() is called (either manually or via the
// periodic flush timer in the source poller).
func (fs *FileStore) Set(table string, offset Offset) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.offsets[table] = offset
	fs.dirty = true
	return nil
}

// Flush writes the current offset map to disk using an atomic write pattern.
// If no changes have been made since the last flush, this is a no-op.
//
// Atomic write sequence:
//  1. Marshal the offset map to JSON with indentation for readability.
//  2. Write to a temporary file in the same directory as the target.
//  3. Sync the temporary file (fsync) to ensure data reaches disk.
//  4. Rename the temporary file to the target (atomic on POSIX).
func (fs *FileStore) Flush() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if !fs.dirty {
		return nil
	}

	data, err := json.MarshalIndent(fs.offsets, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling offsets: %w", err)
	}

	// Ensure the directory exists.
	dir := filepath.Dir(fs.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("creating offset directory: %w", err)
	}

	// Write to a temp file in the same directory (ensures atomic rename will work).
	tmpFile, err := os.CreateTemp(dir, "offsets-*.tmp")
	if err != nil {
		return fmt.Errorf("creating temp offset file: %w", err)
	}
	tmpPath := tmpFile.Name()

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("writing temp offset file: %w", err)
	}

	// Sync to ensure data reaches disk before rename.
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("syncing temp offset file: %w", err)
	}
	tmpFile.Close()

	// Atomic rename — on POSIX this is guaranteed atomic.
	if err := os.Rename(tmpPath, fs.path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("renaming offset file: %w", err)
	}

	fs.dirty = false
	return nil
}

// Close flushes any pending changes and releases resources.
func (fs *FileStore) Close() error {
	return fs.Flush()
}

// Snapshot returns a copy of all stored offsets. This is useful for debugging
// and metrics reporting. The returned map is a copy — mutations do not affect
// the store.
func (fs *FileStore) Snapshot() map[string]Offset {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	snapshot := make(map[string]Offset, len(fs.offsets))
	for k, v := range fs.offsets {
		snapshot[k] = v
	}
	return snapshot
}
