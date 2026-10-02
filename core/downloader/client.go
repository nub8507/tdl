package downloader

import (
	"context"
	"sync"

	"github.com/go-faster/errors"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/zap"

	"github.com/iyear/tdl/core/logctx"
)

// maxRefreshes limits how many times the file reference is refreshed for one part.
const maxRefreshes = 3

// client is the RPC client of a single download. It serves parts which are already
// written to the destination, never asks for data past the end of file (Telegram answers
// OFFSET_INVALID instead of an empty part) and refreshes expired file references
// without losing the download progress.
type client struct {
	downloader.Client

	size    int64
	stored  Resumable   // nil if download can't be continued
	refresh Refreshable // nil if file location can't be refreshed

	mu       sync.Mutex
	location tg.InputFileLocationClass
	gen      int // incremented on every refresh
}

func newClient(rpc downloader.Client, elem Elem) *client {
	c := &client{
		Client:   rpc,
		size:     elem.File().Size(),
		location: elem.File().Location(),
	}

	if r, ok := elem.(Resumable); ok && r.Journal() != nil {
		c.stored = r
	}
	if r, ok := elem.(Refreshable); ok {
		c.refresh = r
	}

	return c
}

func (c *client) UploadGetFile(ctx context.Context, req *tg.UploadGetFileRequest) (tg.UploadFileClass, error) {
	if c.size > 0 && req.Offset >= c.size {
		// empty part means the end of file for downloader
		return &tg.UploadFile{Type: &tg.StorageFileUnknown{}}, nil
	}

	if c.stored != nil && c.stored.Journal().Has(req.Offset) {
		return c.readStored(req.Offset, req.Limit)
	}

	for refreshes := 0; ; refreshes++ {
		location, gen := c.current()

		r := *req
		r.Location = location

		file, err := c.Client.UploadGetFile(ctx, &r)
		if err == nil || c.refresh == nil || refreshes >= maxRefreshes ||
			!tgerr.Is(err, tg.ErrFileReferenceExpired, tg.ErrFileReferenceInvalid) {
			return file, err
		}

		if err = c.refreshLocation(ctx, gen); err != nil {
			return nil, errors.Wrap(err, "refresh file reference")
		}
	}
}

func (c *client) readStored(offset int64, limit int) (tg.UploadFileClass, error) {
	n := int64(limit)
	if c.size > 0 {
		n = min(n, c.size-offset)
	}

	data := make([]byte, n)
	if _, err := c.stored.ReadAt(data, offset); err != nil {
		return nil, errors.Wrapf(err, "read downloaded part at %d", offset)
	}

	return &tg.UploadFile{Type: &tg.StorageFileUnknown{}, Bytes: data}, nil
}

func (c *client) current() (tg.InputFileLocationClass, int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.location, c.gen
}

// refreshLocation fetches new file location, unless it was already
// refreshed by another thread after the failed request was made.
func (c *client) refreshLocation(ctx context.Context, gen int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.gen != gen {
		return nil
	}

	location, err := c.refresh.RefreshLocation(ctx)
	if err != nil {
		return err
	}

	c.location = location
	c.gen++

	logctx.From(ctx).Info("File reference refreshed", zap.Int("refreshes", c.gen))
	return nil
}
