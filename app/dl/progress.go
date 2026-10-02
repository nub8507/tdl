package dl

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/gabriel-vasile/mimetype"
	"github.com/go-faster/errors"
	pw "github.com/jedib0t/go-pretty/v6/progress"
	"go.uber.org/zap"

	"github.com/iyear/tdl/core/downloader"
	"github.com/iyear/tdl/core/logctx"
	"github.com/iyear/tdl/core/util/fsutil"
	"github.com/iyear/tdl/pkg/prog"
	"github.com/iyear/tdl/pkg/utils"
)

type progress struct {
	ctx      context.Context // context of the whole download, done when it's interrupted
	pw       pw.Writer
	trackers *sync.Map // map[ID]*pw.Tracker
	opts     Options

	it *iter
}

func newProgress(ctx context.Context, p pw.Writer, it *iter, opts Options) *progress {
	return &progress{
		ctx:      ctx,
		pw:       p,
		trackers: &sync.Map{},
		opts:     opts,
		it:       it,
	}
}

func (p *progress) OnAdd(elem downloader.Elem) {
	tracker := prog.AppendTracker(p.pw, utils.Byte.FormatBinaryBytes, p.processMessage(elem), elem.File().Size())
	p.trackers.Store(elem.(*iterElem).id, tracker)
}

func (p *progress) OnDownload(elem downloader.Elem, state downloader.ProgressState) {
	tracker, ok := p.trackers.Load(elem.(*iterElem).id)
	if !ok {
		return
	}

	t := tracker.(*pw.Tracker)
	t.UpdateTotal(state.Total)
	t.SetValue(state.Downloaded)
}

func (p *progress) OnDone(elem downloader.Elem, err error) {
	e := elem.(*iterElem)

	tracker, ok := p.trackers.Load(e.id)
	if !ok {
		return
	}
	t := tracker.(*pw.Tracker)

	if cerr := e.close(); cerr != nil && err == nil {
		err = errors.Wrap(cerr, "close file")
	}
	if err == nil && !e.journal.Complete() {
		err = errors.Errorf("incomplete download: %d of %d parts", e.journal.Done(), e.journal.Total())
	}

	log := logctx.From(p.ctx).With(
		zap.String("journal", e.journal.Path()),
		zap.Int("parts", e.journal.Done()),
		zap.Int("total", e.journal.Total()),
	)

	// temp file and its journal are kept to continue the download next time
	if err != nil {
		log.Info("Download journal kept to continue the download", zap.Error(err))
		if p.ctx.Err() == nil { // don't report user cancel
			p.fail(t, elem, errors.Wrap(err, "progress"))
		}
		return
	}

	p.it.Finish(e.logicalPos)

	if err := p.donePost(e); err != nil {
		p.fail(t, elem, errors.Wrap(err, "post file"))
		return
	}

	if err := e.journal.Remove(); err != nil {
		log.Warn("Remove download journal", zap.Error(err))
		p.pw.Log(color.YellowString("%s: remove journal: %s", p.elemString(elem), err.Error()))
	} else {
		log.Info("Download finished, journal removed")
	}

	if !t.IsDone() {
		t.MarkAsDone()
	}
}

func (p *progress) donePost(elem *iterElem) error {
	newfile := strings.TrimSuffix(filepath.Base(elem.to.Name()), tempExt)

	if p.opts.RewriteExt {
		mime, err := mimetype.DetectFile(elem.to.Name())
		if err != nil {
			return errors.Wrap(err, "detect mime")
		}
		ext := mime.Extension()
		if ext != "" && (filepath.Ext(newfile) != ext) {
			newfile = fsutil.GetNameWithoutExt(newfile) + ext
		}
	}

	newpath := filepath.Join(filepath.Dir(elem.to.Name()), newfile)
	if err := os.Rename(elem.to.Name(), newpath); err != nil {
		return errors.Wrap(err, "rename file")
	}

	// Set file modification time to message date if available
	if elem.file.Date > 0 {
		fileTime := time.Unix(elem.file.Date, 0)
		if err := os.Chtimes(newpath, fileTime, fileTime); err != nil {
			return errors.Wrap(err, "set file time")
		}
	}

	return nil
}

func (p *progress) fail(t *pw.Tracker, elem downloader.Elem, err error) {
	p.pw.Log(color.RedString("%s error: %s", p.elemString(elem), err.Error()))
	t.MarkAsErrored()
}

func (p *progress) processMessage(elem downloader.Elem) string {
	return p.elemString(elem)
}

func (p *progress) elemString(elem downloader.Elem) string {
	return elem.(*iterElem).String()
}
