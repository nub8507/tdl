package downloader

import (
	"os"
	"path/filepath"
	"testing"
)

const testFileID = 42

func openTestJournal(t *testing.T, path string, size, id, available int64) *Journal {
	t.Helper()

	j, err := OpenJournal(path, size, id, available)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	t.Cleanup(func() { _ = j.Close() })

	return j
}

func markParts(t *testing.T, j *Journal, parts ...int64) {
	t.Helper()

	for _, p := range parts {
		if err := j.Mark(p * MaxPartSize); err != nil {
			t.Fatalf("mark part %d: %v", p, err)
		}
	}
}

func TestJournalReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file.journal")
	size := int64(3*MaxPartSize + 100)

	j := openTestJournal(t, path, size, testFileID, 0)
	if j.Total() != 4 || j.Done() != 0 || j.Discarded() {
		t.Fatalf("new journal: total %d, done %d, discarded %v", j.Total(), j.Done(), j.Discarded())
	}
	if j.Path() != path {
		t.Fatalf("journal path: %s", j.Path())
	}
	markParts(t, j, 0, 2, 2)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	j = openTestJournal(t, path, size, testFileID, size)
	if j.Done() != 2 || !j.Has(0) || j.Has(MaxPartSize) || !j.Has(2*MaxPartSize) || j.Complete() || j.Discarded() {
		t.Fatalf("reopened journal: done %d, discarded %v", j.Done(), j.Discarded())
	}

	markParts(t, j, 1, 3)
	if !j.Complete() {
		t.Fatalf("journal must be complete: done %d of %d", j.Done(), j.Total())
	}
}

func TestJournalReset(t *testing.T) {
	size := int64(2 * MaxPartSize)

	tests := []struct {
		name            string
		size, id, avail int64
	}{
		{name: "other file", size: size, id: testFileID + 1, avail: size},
		{name: "other size", size: size + 1, id: testFileID, avail: size + 1},
		{name: "destination is shorter", size: size, id: testFileID, avail: 2*MaxPartSize - 1},
		{name: "restart", size: size, id: testFileID, avail: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "file.journal")

			j := openTestJournal(t, path, size, testFileID, 0)
			markParts(t, j, 0, 1)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}

			j = openTestJournal(t, path, tt.size, tt.id, tt.avail)
			if j.Done() != 0 || !j.Discarded() {
				t.Fatalf("journal must be reset, done %d, discarded %v", j.Done(), j.Discarded())
			}

			// reset journal must describe the new file
			markParts(t, j, 1)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			j = openTestJournal(t, path, tt.size, tt.id, tt.size)
			if j.Done() != 1 || !j.Has(MaxPartSize) {
				t.Fatalf("journal after reset: done %d", j.Done())
			}
		})
	}
}

func TestJournalTornRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file.journal")
	size := int64(3 * MaxPartSize)

	j := openTestJournal(t, path, size, testFileID, 0)
	markParts(t, j, 0)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	// simulate write interrupted in the middle of the record
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte{2, 0}); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}

	j = openTestJournal(t, path, size, testFileID, size)
	if j.Done() != 1 || !j.Has(0) || j.Discarded() {
		t.Fatalf("torn record must be ignored, done %d, discarded %v", j.Done(), j.Discarded())
	}
	markParts(t, j, 2)
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}

	j = openTestJournal(t, path, size, testFileID, size)
	if j.Done() != 2 || !j.Has(0) || !j.Has(2*MaxPartSize) {
		t.Fatalf("records after torn one: done %d", j.Done())
	}
}

func TestJournalCorrupted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file.journal")
	size := int64(2 * MaxPartSize)

	j := openTestJournal(t, path, size, testFileID, 0)
	markParts(t, j, 0)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	// index of the part which doesn't exist
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte{100, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}

	j = openTestJournal(t, path, size, testFileID, size)
	if j.Done() != 0 || !j.Discarded() {
		t.Fatalf("corrupted journal must be reset, done %d, discarded %v", j.Done(), j.Discarded())
	}
}

func TestJournalUnexpectedOffset(t *testing.T) {
	j := openTestJournal(t, filepath.Join(t.TempDir(), "file.journal"), 2*MaxPartSize, testFileID, 0)

	for _, offset := range []int64{-MaxPartSize, 1, MaxPartSize + 1, 2 * MaxPartSize} {
		if err := j.Mark(offset); err == nil {
			t.Errorf("offset %d must be rejected", offset)
		}
		if j.Has(offset) {
			t.Errorf("offset %d must not be stored", offset)
		}
	}
}

func TestJournalRemove(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file.journal")

	j, err := OpenJournal(path, MaxPartSize, testFileID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	if err = j.Remove(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("journal must be removed: %v", err)
	}
}
