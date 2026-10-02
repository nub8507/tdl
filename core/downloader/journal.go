package downloader

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/go-faster/errors"
)

const (
	journalMagic   = "TDLJ"
	journalVersion = 1
	// magic, version, part size, file size, file id
	journalHeaderSize = 4 + 4 + 8 + 8 + 8
	journalRecordSize = 4
)

// Journal records which parts of the file are already written to the destination,
// so an interrupted download can be continued instead of starting from scratch.
//
// It is an append-only file: a header describing the downloaded file
// followed by little-endian uint32 indexes of the written parts.
type Journal struct {
	mu        sync.Mutex
	f         *os.File
	parts     []bool
	done      int
	discarded bool // journal of the previous download couldn't be used
}

// OpenJournal opens the journal of the file with given size and id, creating it if needed.
// Previously recorded parts are kept only if the journal describes the same file
// and all of them are within the first available bytes of the destination,
// otherwise the journal is started from scratch.
func OpenJournal(path string, size, id, available int64) (_ *Journal, rerr error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, errors.Wrap(err, "open journal")
	}
	defer func() {
		if rerr != nil {
			_ = f.Close()
		}
	}()

	j := &Journal{f: f, parts: make([]bool, partsCount(size))}

	header := journalHeader(size, id)
	ok, err := j.load(header, size, available)
	if err != nil {
		return nil, errors.Wrap(err, "load journal")
	}
	if ok {
		return j, nil
	}

	if err = j.reset(header); err != nil {
		return nil, errors.Wrap(err, "reset journal")
	}
	return j, nil
}

func partsCount(size int64) int {
	if size <= 0 {
		return 0
	}
	return int((size + MaxPartSize - 1) / MaxPartSize)
}

func journalHeader(size, id int64) []byte {
	b := make([]byte, journalHeaderSize)
	copy(b, journalMagic)
	binary.LittleEndian.PutUint32(b[4:], journalVersion)
	binary.LittleEndian.PutUint64(b[8:], uint64(MaxPartSize))
	binary.LittleEndian.PutUint64(b[16:], uint64(size))
	binary.LittleEndian.PutUint64(b[24:], uint64(id))
	return b
}

// load reads parts recorded by previous downloads.
// It reports false if there are none or they can't be used to continue the download.
func (j *Journal) load(header []byte, size, available int64) (bool, error) {
	data, err := io.ReadAll(j.f)
	if err != nil {
		return false, err
	}
	if len(data) == 0 { // new journal
		return false, nil
	}

	n, ok := j.parse(data, header, size, available)
	if !ok {
		j.discarded = true
		return false, nil
	}

	// drop the torn record left by an interrupted write, so new records stay aligned
	if want := int64(journalHeaderSize + n*journalRecordSize); int64(len(data)) != want {
		if err = j.f.Truncate(want); err != nil {
			return false, err
		}
	}

	return true, nil
}

// parse fills parts from the journal data and returns the number of whole records.
// It reports false if the data can't be used to continue the download.
func (j *Journal) parse(data, header []byte, size, available int64) (int, bool) {
	if len(data) < journalHeaderSize || !bytes.Equal(data[:journalHeaderSize], header) {
		return 0, false
	}

	records := data[journalHeaderSize:]
	n := len(records) / journalRecordSize
	for k := 0; k < n; k++ {
		idx := int64(binary.LittleEndian.Uint32(records[k*journalRecordSize:]))
		if idx >= int64(len(j.parts)) {
			return 0, false // corrupted
		}
		if min((idx+1)*MaxPartSize, size) > available {
			return 0, false // destination doesn't contain the part anymore
		}
		if !j.parts[idx] {
			j.parts[idx] = true
			j.done++
		}
	}

	return n, true
}

func (j *Journal) reset(header []byte) error {
	clear(j.parts)
	j.done = 0

	if err := j.f.Truncate(0); err != nil {
		return err
	}
	_, err := j.f.Write(header)
	return err
}

func (j *Journal) index(offset int64) (int, bool) {
	if offset < 0 || offset%MaxPartSize != 0 || offset/MaxPartSize >= int64(len(j.parts)) {
		return 0, false
	}
	return int(offset / MaxPartSize), true
}

// Has reports whether the part starting at offset is already written.
func (j *Journal) Has(offset int64) bool {
	idx, ok := j.index(offset)
	if !ok {
		return false
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	return j.parts[idx]
}

// Mark records that the part starting at offset is written to the destination.
func (j *Journal) Mark(offset int64) error {
	idx, ok := j.index(offset)
	if !ok {
		if len(j.parts) == 0 { // size of the file is unknown, nothing to record
			return nil
		}
		return errors.Errorf("unexpected part offset %d", offset)
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	if j.parts[idx] {
		return nil
	}

	record := make([]byte, journalRecordSize)
	binary.LittleEndian.PutUint32(record, uint32(idx))
	if _, err := j.f.Write(record); err != nil {
		return errors.Wrap(err, "write journal")
	}

	j.parts[idx] = true
	j.done++
	return nil
}

// Done returns the number of written parts.
func (j *Journal) Done() int {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.done
}

// Total returns the number of parts of the file.
func (j *Journal) Total() int {
	return len(j.parts)
}

// Complete reports whether all parts of the file are written.
func (j *Journal) Complete() bool {
	return j.Done() == j.Total()
}

// Path returns path of the journal file.
func (j *Journal) Path() string {
	return j.f.Name()
}

// Discarded reports whether the journal left by the previous download
// couldn't be used, so the download is started from scratch.
func (j *Journal) Discarded() bool {
	return j.discarded
}

// Close closes the journal and keeps it on disk.
func (j *Journal) Close() error {
	return j.f.Close()
}

// Remove deletes the journal file. It should be called after Close.
func (j *Journal) Remove() error {
	return os.Remove(j.f.Name())
}
