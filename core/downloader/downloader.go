package downloader

import (
	"context"
	"fmt"

	"github.com/go-faster/errors"
	"github.com/gotd/td/telegram/downloader"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/iyear/tdl/core/dcpool"
	"github.com/iyear/tdl/core/logctx"
	"github.com/iyear/tdl/core/util/tutil"
)

// MaxPartSize refer to https://core.telegram.org/api/files#downloading-files
const MaxPartSize = 1024 * 1024

type Downloader struct {
	opts Options
}

type Options struct {
	Pool     dcpool.Pool
	Threads  int
	Iter     Iter
	Progress Progress
}

func New(opts Options) *Downloader {
	return &Downloader{
		opts: opts,
	}
}

func (d *Downloader) Download(ctx context.Context, limit int) error {
	wg, wgctx := errgroup.WithContext(ctx)
	wg.SetLimit(limit)

	failed := atomic.NewInt64(0)
	for d.opts.Iter.Next(wgctx) {
		elem := d.opts.Iter.Value()

		wg.Go(func() error {
			d.opts.Progress.OnAdd(elem)

			err := d.download(wgctx, elem)
			d.opts.Progress.OnDone(elem, err)
			if err == nil {
				return nil
			}

			// interrupted by user, so we directly return error to stop all.
			// Errors of the connection may also wrap context.Canceled, so check the context itself.
			if ctx.Err() != nil {
				return errors.Wrap(err, "download")
			}

			// don't stop other downloads, just log the error
			failed.Inc()
			logctx.
				From(ctx).
				Error("Download error",
					zap.String("element", elemString(elem)),
					zap.Error(err),
				)

			return nil
		})
	}

	if err := d.opts.Iter.Err(); err != nil {
		return errors.Wrap(err, "iter")
	}

	if err := wg.Wait(); err != nil {
		return err
	}

	if n := failed.Load(); n > 0 {
		return errors.Errorf("failed to download %d file(s), see log for details", n)
	}

	return nil
}

func elemString(elem Elem) string {
	if s, ok := elem.(fmt.Stringer); ok {
		return s.String()
	}
	return fmt.Sprintf("%T", elem)
}

func (d *Downloader) download(ctx context.Context, elem Elem) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	logctx.From(ctx).Debug("Start download elem",
		zap.String("elem", elemString(elem)))

	client := d.opts.Pool.Client(ctx, elem.File().DC())
	if elem.AsTakeout() {
		client = d.opts.Pool.Takeout(ctx, elem.File().DC())
	}

	_, err := downloader.NewDownloader().WithPartSize(MaxPartSize).
		Download(newClient(client, elem), elem.File().Location()).
		WithThreads(tutil.BestThreads(elem.File().Size(), d.opts.Threads)).
		Parallel(ctx, newWriteAt(elem, d.opts.Progress, MaxPartSize))
	if err != nil {
		return errors.Wrap(err, "download")
	}

	return nil
}
