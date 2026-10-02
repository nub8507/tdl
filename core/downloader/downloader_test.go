package downloader

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-faster/errors"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/atomic"
)

// fakeTelegram serves upload.getFile like Telegram does: empty part at the end of file,
// OFFSET_INVALID past it and FILE_REFERENCE_EXPIRED for the stale file reference.
type fakeTelegram struct {
	data []byte

	mu        sync.Mutex
	reference []byte
	expireAt  int   // reference expires after this number of served parts, 0 means never
	err       error // returned for every request if set
	served    int
	requested map[int64]int // offsets of served parts
	pastEnd   int           // requests at or past the end of file
}

func newFakeTelegram(size int) *fakeTelegram {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}

	return &fakeTelegram{
		data:      data,
		reference: []byte("ref-0"),
		requested: make(map[int64]int),
	}
}

func (f *fakeTelegram) currentReference() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.reference
}

func (f *fakeTelegram) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	req, ok := input.(*tg.UploadGetFileRequest)
	if !ok {
		return errors.Errorf("unexpected request %T", input)
	}
	location, ok := req.Location.(*tg.InputDocumentFileLocation)
	if !ok {
		return errors.Errorf("unexpected location %T", req.Location)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return f.err
	}
	if !bytes.Equal(location.FileReference, f.reference) {
		return tgerr.New(400, tg.ErrFileReferenceExpired)
	}

	size := int64(len(f.data))
	if req.Offset >= size {
		f.pastEnd++
		if req.Offset > size {
			return tgerr.New(400, tg.ErrOffsetInvalid)
		}
		return encode(&tg.UploadFile{Type: &tg.StorageFileUnknown{}}, output)
	}

	f.requested[req.Offset]++
	f.served++
	if f.served == f.expireAt {
		f.reference = []byte("ref-1")
	}

	end := min(req.Offset+int64(req.Limit), size)
	return encode(&tg.UploadFile{Type: &tg.StorageFileUnknown{}, Bytes: f.data[req.Offset:end]}, output)
}

func encode(file *tg.UploadFile, output bin.Decoder) error {
	var b bin.Buffer
	if err := file.Encode(&b); err != nil {
		return err
	}
	return output.Decode(&b)
}

type testElem struct {
	srv      *fakeTelegram
	location tg.InputFileLocationClass
	size     int64

	to      *os.File
	journal *Journal

	refreshErr error
	refreshes  atomic.Int64

	done    bool  // OnDone was called
	doneErr error // error passed to OnDone
}

func (e *testElem) File() File                          { return e }
func (e *testElem) To() io.WriterAt                     { return e.to }
func (e *testElem) AsTakeout() bool                     { return false }
func (e *testElem) Location() tg.InputFileLocationClass { return e.location }
func (e *testElem) Size() int64                         { return e.size }
func (e *testElem) DC() int                             { return 2 }
func (e *testElem) Journal() *Journal                   { return e.journal }

func (e *testElem) ReadAt(p []byte, off int64) (int, error) { return e.to.ReadAt(p, off) }

func (e *testElem) RefreshLocation(context.Context) (tg.InputFileLocationClass, error) {
	e.refreshes.Inc()
	if e.refreshErr != nil {
		return nil, e.refreshErr
	}
	return &tg.InputDocumentFileLocation{ID: testFileID, FileReference: e.srv.currentReference()}, nil
}

// newTestElem opens destination and journal like a new run of tdl does.
func newTestElem(t *testing.T, srv *fakeTelegram, path string) *testElem {
	t.Helper()

	to, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = to.Close() })

	stat, err := to.Stat()
	if err != nil {
		t.Fatal(err)
	}

	journal := openTestJournal(t, path+".journal", int64(len(srv.data)), testFileID, stat.Size())

	return &testElem{
		srv:      srv,
		location: &tg.InputDocumentFileLocation{ID: testFileID, FileReference: []byte("ref-0")},
		size:     int64(len(srv.data)),
		to:       to,
		journal:  journal,
	}
}

type testPool struct{ client *tg.Client }

func (p testPool) Client(context.Context, int) *tg.Client  { return p.client }
func (p testPool) Takeout(context.Context, int) *tg.Client { return p.client }
func (p testPool) Default(context.Context) *tg.Client      { return p.client }
func (p testPool) Close() error                            { return nil }

type testIter struct {
	elems []Elem
	next  int
}

func (it *testIter) Next(context.Context) bool {
	if it.next >= len(it.elems) {
		return false
	}
	it.next++
	return true
}

func (it *testIter) Value() Elem { return it.elems[it.next-1] }

func (it *testIter) Err() error { return nil }

type testProgress struct{}

func (testProgress) OnAdd(Elem)                     {}
func (testProgress) OnDownload(Elem, ProgressState) {}
func (testProgress) OnDone(elem Elem, err error) {
	e := elem.(*testElem)
	e.done, e.doneErr = true, err
}

// runDownload downloads the element, its own error is stored in doneErr.
func runDownload(t *testing.T, srv *fakeTelegram, elem *testElem) error {
	t.Helper()

	err := New(Options{
		Pool:     testPool{client: tg.NewClient(srv)},
		Threads:  4,
		Iter:     &testIter{elems: []Elem{elem}},
		Progress: testProgress{},
	}).Download(context.Background(), 1)

	if !elem.done {
		t.Fatal("OnDone wasn't called")
	}
	return err
}

func checkDownloaded(t *testing.T, srv *fakeTelegram, elem *testElem) {
	t.Helper()

	got, err := os.ReadFile(elem.to.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, srv.data) {
		t.Fatalf("downloaded data differs: got %d bytes, want %d", len(got), len(srv.data))
	}
	if !elem.journal.Complete() {
		t.Fatalf("journal isn't complete: %d of %d", elem.journal.Done(), elem.journal.Total())
	}
	for offset, n := range srv.requested {
		if n != 1 {
			t.Errorf("part at %d requested %d times", offset, n)
		}
	}
}

func TestDownloadEndOfFile(t *testing.T) {
	for _, size := range []int{1, MaxPartSize, 4 * MaxPartSize, 5*MaxPartSize + MaxPartSize/2} {
		srv := newFakeTelegram(size)
		elem := newTestElem(t, srv, filepath.Join(t.TempDir(), "file.tmp"))

		err := runDownload(t, srv, elem)
		if err != nil || elem.doneErr != nil {
			t.Fatalf("size %d: download: %v, element: %v", size, err, elem.doneErr)
		}

		checkDownloaded(t, srv, elem)
		if srv.pastEnd != 0 {
			t.Errorf("size %d: %d requests past the end of file", size, srv.pastEnd)
		}
	}
}

func TestDownloadUnknownSize(t *testing.T) {
	srv := newFakeTelegram(MaxPartSize + 10)
	elem := newTestElem(t, srv, filepath.Join(t.TempDir(), "file.tmp"))
	// size of the file is unknown
	elem.size = 0
	if err := elem.journal.Close(); err != nil {
		t.Fatal(err)
	}
	elem.journal = openTestJournal(t, elem.to.Name()+".journal", 0, testFileID, 0)

	err := runDownload(t, srv, elem)
	if err != nil || elem.doneErr != nil {
		t.Fatalf("download: %v, element: %v", err, elem.doneErr)
	}

	got, err := os.ReadFile(elem.to.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, srv.data) {
		t.Fatalf("downloaded data differs: got %d bytes, want %d", len(got), len(srv.data))
	}
}

func TestDownloadRefreshesExpiredReference(t *testing.T) {
	srv := newFakeTelegram(5*MaxPartSize + 123)
	srv.expireAt = 2
	elem := newTestElem(t, srv, filepath.Join(t.TempDir(), "file.tmp"))

	err := runDownload(t, srv, elem)
	if err != nil || elem.doneErr != nil {
		t.Fatalf("download: %v, element: %v", err, elem.doneErr)
	}

	checkDownloaded(t, srv, elem)
	// all threads get the error, but the reference is fetched only once
	if n := elem.refreshes.Load(); n != 1 {
		t.Errorf("reference refreshed %d times", n)
	}
}

func TestDownloadRefreshFailure(t *testing.T) {
	srv := newFakeTelegram(5 * MaxPartSize)
	srv.expireAt = 1
	elem := newTestElem(t, srv, filepath.Join(t.TempDir(), "file.tmp"))
	elem.refreshErr = errors.New("message was deleted")

	err := runDownload(t, srv, elem)
	if err == nil {
		t.Fatal("download must fail")
	}
	if !errors.Is(elem.doneErr, elem.refreshErr) {
		t.Fatalf("element error: %v", elem.doneErr)
	}
	if elem.journal.Complete() {
		t.Fatal("journal must not be complete")
	}
}

func TestDownloadReportsError(t *testing.T) {
	srv := newFakeTelegram(3 * MaxPartSize)
	srv.err = tgerr.New(400, tg.ErrFileIDInvalid)
	elem := newTestElem(t, srv, filepath.Join(t.TempDir(), "file.tmp"))

	err := runDownload(t, srv, elem)
	if err == nil {
		t.Fatal("download must fail")
	}
	if !tgerr.Is(elem.doneErr, tg.ErrFileIDInvalid) {
		t.Fatalf("element error: %v", elem.doneErr)
	}
	if n := elem.refreshes.Load(); n != 0 {
		t.Errorf("reference refreshed %d times", n)
	}
}

func TestDownloadResume(t *testing.T) {
	srv := newFakeTelegram(5*MaxPartSize + 1000)
	path := filepath.Join(t.TempDir(), "file.tmp")

	// previous run wrote parts 0, 2 and 3, part 1 was written but not recorded
	prev := newTestElem(t, srv, path)
	for _, part := range []int64{0, 1, 2, 3} {
		off := part * MaxPartSize
		if _, err := prev.to.WriteAt(srv.data[off:off+MaxPartSize], off); err != nil {
			t.Fatal(err)
		}
		if part != 1 {
			markParts(t, prev.journal, part)
		}
	}
	if err := prev.journal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := prev.to.Close(); err != nil {
		t.Fatal(err)
	}

	elem := newTestElem(t, srv, path)
	if elem.journal.Done() != 3 {
		t.Fatalf("journal must keep 3 parts, got %d", elem.journal.Done())
	}

	err := runDownload(t, srv, elem)
	if err != nil || elem.doneErr != nil {
		t.Fatalf("download: %v, element: %v", err, elem.doneErr)
	}

	checkDownloaded(t, srv, elem)
	for _, part := range []int64{0, 2, 3} {
		if n := srv.requested[part*MaxPartSize]; n != 0 {
			t.Errorf("stored part %d requested %d times", part, n)
		}
	}
	for _, part := range []int64{1, 4, 5} {
		if n := srv.requested[part*MaxPartSize]; n != 1 {
			t.Errorf("missing part %d requested %d times", part, n)
		}
	}
}
