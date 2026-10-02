package dl

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/go-faster/errors"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/tg"
	"go.uber.org/multierr"

	"github.com/iyear/tdl/core/dcpool"
	"github.com/iyear/tdl/core/downloader"
	"github.com/iyear/tdl/core/tmedia"
	"github.com/iyear/tdl/core/util/tutil"
)

type iterElem struct {
	id         int // tracker id for progress tracking
	logicalPos int // logical position for resume/finished tracking

	from    peers.Peer
	fromMsg *tg.Message
	file    *tmedia.Media

	to      *os.File
	journal *downloader.Journal

	pool dcpool.Pool
	opts Options
}

func (i *iterElem) File() downloader.File { return i }

func (i *iterElem) To() io.WriterAt { return i.to }

func (i *iterElem) AsTakeout() bool { return i.opts.Takeout }

func (i *iterElem) Location() tg.InputFileLocationClass { return i.file.InputFileLoc }

func (i *iterElem) Name() string { return i.file.Name }

func (i *iterElem) Size() int64 { return i.file.Size }

func (i *iterElem) DC() int { return i.file.DC }

func (i *iterElem) Journal() *downloader.Journal { return i.journal }

func (i *iterElem) ReadAt(p []byte, off int64) (int, error) { return i.to.ReadAt(p, off) }

// RefreshLocation fetches the message again to get the file location with a fresh file reference.
func (i *iterElem) RefreshLocation(ctx context.Context) (tg.InputFileLocationClass, error) {
	msg, err := tutil.GetSingleMessage(ctx, i.pool.Default(ctx), i.from.InputPeer(), i.fromMsg.ID)
	if err != nil {
		return nil, errors.Wrap(err, "get message")
	}

	media, ok := tmedia.GetMedia(msg)
	if !ok {
		return nil, errors.New("message has no media anymore")
	}

	oldID, ok1 := tmedia.LocationID(i.file.InputFileLoc)
	newID, ok2 := tmedia.LocationID(media.InputFileLoc)
	if ok1 && ok2 && oldID != newID {
		return nil, errors.New("media of the message was changed")
	}

	return media.InputFileLoc, nil
}

// close closes the destination file and the journal, both are kept on disk.
func (i *iterElem) close() error {
	return multierr.Combine(i.to.Close(), i.journal.Close())
}

func (i *iterElem) String() string {
	return fmt.Sprintf("%s(%d):%d -> %s",
		i.from.VisibleName(),
		i.from.ID(),
		i.fromMsg.ID,
		strings.TrimSuffix(i.to.Name(), tempExt))
}
