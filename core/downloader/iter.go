package downloader

import (
	"context"
	"io"

	"github.com/gotd/td/tg"
)

type Iter interface {
	Next(ctx context.Context) bool
	Value() Elem
	Err() error
}

type Elem interface {
	File() File
	To() io.WriterAt

	AsTakeout() bool
}

type File interface {
	Location() tg.InputFileLocationClass
	Size() int64
	DC() int
}

// Resumable is implemented by Elem which keeps a journal of written parts,
// so the download can be continued after it was interrupted.
type Resumable interface {
	// Journal returns the journal of written parts or nil.
	Journal() *Journal
	// ReadAt reads already written data back from the destination.
	ReadAt(p []byte, off int64) (int, error)
}

// Refreshable is implemented by Elem whose file location can be fetched again
// when Telegram reports that the file reference has expired.
type Refreshable interface {
	RefreshLocation(ctx context.Context) (tg.InputFileLocationClass, error)
}
