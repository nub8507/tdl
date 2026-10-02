package dl

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/iyear/tdl/core/downloader"
	"github.com/iyear/tdl/core/tmedia"
)

func testMedia() *tmedia.Media {
	return &tmedia.Media{
		InputFileLoc: &tg.InputDocumentFileLocation{ID: 1},
		Size:         3 * downloader.MaxPartSize,
	}
}

func fileSize(t *testing.T, f *os.File) int64 {
	t.Helper()

	stat, err := f.Stat()
	require.NoError(t, err)
	return stat.Size()
}

func TestOpenFileJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file.rar"+tempExt)
	it := &iter{}

	// journal is created next to the temp file as soon as the file is queued
	to, journal, err := it.openFile(path, testMedia())
	require.NoError(t, err)
	assert.FileExists(t, path+journalExt)
	assert.Equal(t, path+journalExt, journal.Path())
	assert.Zero(t, journal.Done())

	// the first part is downloaded, then the download is interrupted
	_, err = to.WriteAt(bytes.Repeat([]byte{1}, downloader.MaxPartSize), 0)
	require.NoError(t, err)
	require.NoError(t, journal.Mark(0))
	require.NoError(t, to.Close())
	require.NoError(t, journal.Close())

	// the next run continues the download
	to, journal, err = it.openFile(path, testMedia())
	require.NoError(t, err)
	assert.Equal(t, 1, journal.Done())
	assert.EqualValues(t, downloader.MaxPartSize, fileSize(t, to))
	require.NoError(t, to.Close())
	require.NoError(t, journal.Close())

	// restart drops the downloaded data
	it.opts.Restart = true
	to, journal, err = it.openFile(path, testMedia())
	require.NoError(t, err)
	assert.Zero(t, journal.Done())
	assert.True(t, journal.Discarded())
	assert.Zero(t, fileSize(t, to))
	require.NoError(t, to.Close())
	require.NoError(t, journal.Close())
}

func TestOpenFileWithoutJournal(t *testing.T) {
	// temp file left by tdl without journal can't be trusted
	path := filepath.Join(t.TempDir(), "file.rar"+tempExt)
	require.NoError(t, os.WriteFile(path, []byte("partially downloaded"), 0o644))

	to, journal, err := (&iter{}).openFile(path, testMedia())
	require.NoError(t, err)
	assert.Zero(t, journal.Done())
	assert.Zero(t, fileSize(t, to))
	require.NoError(t, to.Close())
	require.NoError(t, journal.Close())
}
