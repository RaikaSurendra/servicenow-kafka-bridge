package offset

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestNewFileStore_EmptyStart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offsets.json")

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore failed: %v", err)
	}
	defer store.Close()

	off, err := store.Get("incident")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !off.IsZero() {
		t.Errorf("expected zero offset, got %+v", off)
	}
}

func TestFileStore_SetGetFlush(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offsets.json")

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore failed: %v", err)
	}

	now := time.Now().UTC()
	off := Offset{
		Timestamp:      now.Unix(),
		LastIdentifier: "abc123",
	}

	if err := store.Set("incident", off); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Before flush — should be in memory
	got, err := store.Get("incident")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.Timestamp != off.Timestamp || got.LastIdentifier != off.LastIdentifier {
		t.Errorf("Get after Set: got %+v, want %+v", got, off)
	}

	// Flush to disk
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Read the file and verify JSON content
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading offset file: %v", err)
	}

	var onDisk map[string]Offset
	if err := json.Unmarshal(data, &onDisk); err != nil {
		t.Fatalf("parsing offset file: %v", err)
	}
	if onDisk["incident"].Timestamp != off.Timestamp {
		t.Errorf("on-disk timestamp = %d, want %d", onDisk["incident"].Timestamp, off.Timestamp)
	}

	store.Close()
}

func TestFileStore_Persistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offsets.json")

	// Write offsets
	store1, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	store1.Set("incident", Offset{Timestamp: 1000, LastIdentifier: "id1"})
	store1.Set("change_request", Offset{Timestamp: 2000, LastIdentifier: "id2"})
	store1.Close() // writes to disk

	// Re-open and verify
	store2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	off1, _ := store2.Get("incident")
	if off1.Timestamp != 1000 || off1.LastIdentifier != "id1" {
		t.Errorf("incident offset mismatch: %+v", off1)
	}
	off2, _ := store2.Get("change_request")
	if off2.Timestamp != 2000 || off2.LastIdentifier != "id2" {
		t.Errorf("change_request offset mismatch: %+v", off2)
	}
}

func TestFileStore_FlushNoDirtyNoop(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offsets.json")

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Flush with no changes — should not create the file
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// File should not exist since nothing was set
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("expected no file for empty flush, but file exists")
	}
}

func TestFileStore_ConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offsets.json")

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var wg sync.WaitGroup
	tables := []string{"t1", "t2", "t3", "t4", "t5"}

	// Concurrent writes
	for _, table := range tables {
		wg.Add(1)
		go func(tbl string) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				store.Set(tbl, Offset{
					Timestamp:      int64(i),
					LastIdentifier: "id",
				})
			}
		}(table)
	}

	// Concurrent reads
	for _, table := range tables {
		wg.Add(1)
		go func(tbl string) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				store.Get(tbl)
			}
		}(table)
	}

	wg.Wait()

	// Verify all tables have offsets
	snap := store.Snapshot()
	for _, table := range tables {
		if _, ok := snap[table]; !ok {
			t.Errorf("missing offset for table %s", table)
		}
	}
}

func TestFileStore_Snapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offsets.json")

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	store.Set("t1", Offset{Timestamp: 100, LastIdentifier: "a"})
	store.Set("t2", Offset{Timestamp: 200, LastIdentifier: "b"})

	snap := store.Snapshot()

	// Modify snapshot — should not affect store
	snap["t1"] = Offset{Timestamp: 999, LastIdentifier: "modified"}

	got, _ := store.Get("t1")
	if got.Timestamp != 100 {
		t.Error("snapshot modification should not affect store")
	}
}

func TestOffset_Time(t *testing.T) {
	off := Offset{Timestamp: 1706140800, LastIdentifier: "abc"}
	want := time.Unix(1706140800, 0).UTC()
	if off.Time() != want {
		t.Errorf("Time() = %v, want %v", off.Time(), want)
	}
}

func TestOffset_IsZero(t *testing.T) {
	zero := Offset{}
	if !zero.IsZero() {
		t.Error("empty offset should be zero")
	}
	nonZero := Offset{Timestamp: 1}
	if nonZero.IsZero() {
		t.Error("non-empty offset should not be zero")
	}
}

func TestFileStore_AtomicWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offsets.json")

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}

	// Set and flush
	store.Set("t1", Offset{Timestamp: 100, LastIdentifier: "a"})
	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}

	// Verify no temp files left behind
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		if entry.Name() != "offsets.json" {
			t.Errorf("unexpected file left behind: %s", entry.Name())
		}
	}

	store.Close()
}

func TestNewFileStore_CorruptedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offsets.json")

	// Write invalid JSON
	os.WriteFile(path, []byte("{invalid json"), 0644)

	_, err := NewFileStore(path)
	if err == nil {
		t.Fatal("expected error for corrupted file")
	}
}
