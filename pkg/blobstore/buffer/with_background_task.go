package buffer

import (
	"io"

	"google.golang.org/protobuf/proto"
)

// BackgroundTask is a handle returned by WithBackgroundTask(). The
// Finish() function must be called exactly once to indicate the
// background task associated with the buffer is completed.
type BackgroundTask struct {
	completion chan struct{}
	err        error
}

// Finish the background task.
func (t *BackgroundTask) Finish(err error) {
	t.err = err
	close(t.completion)
}

type bufferWithBackgroundTask struct {
	base Buffer
	task *BackgroundTask
}

// WithBackgroundTask returns a decorated Buffer which will at the end
// of its lifetime wait for the completion of a background task. The
// error of the background task is returned if the Buffer itself yields
// no other error.
//
// This function may be used by implementations of BlobAccess that
// multiplex data, ensuring that the foreground goroutine will at some
// point block, waiting for background synchronizations to complete.
// This ensures that the number of concurrent goroutines remains
// bounded and that any synchronization errors can be propagated.
func WithBackgroundTask(b Buffer) (Buffer, *BackgroundTask) {
	t := &BackgroundTask{
		completion: make(chan struct{}),
	}
	return &bufferWithBackgroundTask{
		base: b,
		task: t,
	}, t
}

func (b *bufferWithBackgroundTask) decorateBuffer(replacement Buffer) Buffer {
	return &bufferWithBackgroundTask{
		base: replacement,
		task: b.task,
	}
}

func (b *bufferWithBackgroundTask) decorateChunkReader(r ChunkReader) ChunkReader {
	return &chunkReaderWithBackgroundTask{
		r:    r,
		task: b.task,
	}
}

func (b *bufferWithBackgroundTask) decorateReader(r io.ReadCloser) io.ReadCloser {
	return &readerWithBackgroundTask{
		ReadCloser: r,
		task:       b.task,
	}
}

func (b *bufferWithBackgroundTask) GetSizeBytes() (int64, error) {
	return b.base.GetSizeBytes()
}

func (b *bufferWithBackgroundTask) IntoWriter(w io.Writer) error {
	err := b.base.IntoWriter(w)
	<-b.task.completion
	if err != nil {
		return err
	}
	return b.task.err
}

func (b *bufferWithBackgroundTask) ReadAt(p []byte, off int64) (int, error) {
	n, err := b.base.ReadAt(p, off)
	<-b.task.completion
	if err != nil {
		return n, err
	}
	return n, b.task.err
}

func (b *bufferWithBackgroundTask) ToProto(m proto.Message, maximumSizeBytes int) (proto.Message, error) {
	mResult, err := b.base.ToProto(m, maximumSizeBytes)
	<-b.task.completion
	if err != nil {
		return nil, err
	}
	return mResult, b.task.err
}

func (b *bufferWithBackgroundTask) ToByteSlice(maximumSizeBytes int) ([]byte, error) {
	data, err := b.base.ToByteSlice(maximumSizeBytes)
	<-b.task.completion
	if err != nil {
		return nil, err
	}
	return data, b.task.err
}

func (b *bufferWithBackgroundTask) ToChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	return b.decorateChunkReader(b.base.ToChunkReader(off, maximumChunkSizeBytes))
}

func (b *bufferWithBackgroundTask) ToReader() io.ReadCloser {
	return b.decorateReader(b.base.ToReader())
}

func (b *bufferWithBackgroundTask) CloneCopy(maximumSizeBytes int) (Buffer, Buffer) {
	b1, b2 := b.base.CloneCopy(maximumSizeBytes)
	return b.decorateBuffer(b1), b.decorateBuffer(b2)
}

func (b *bufferWithBackgroundTask) CloneStream() (Buffer, Buffer) {
	b1, b2 := b.base.CloneStream()
	return b.decorateBuffer(b1), b.decorateBuffer(b2)
}

func (b *bufferWithBackgroundTask) Discard() {
	b.base.Discard()
	<-b.task.completion
}

func (b *bufferWithBackgroundTask) applyErrorHandler(errorHandler ErrorHandler) (Buffer, bool) {
	replacement, shouldRetry := b.base.applyErrorHandler(errorHandler)
	return b.decorateBuffer(replacement), shouldRetry
}

func (b *bufferWithBackgroundTask) toUnvalidatedChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	return b.decorateChunkReader(b.base.toUnvalidatedChunkReader(off, maximumChunkSizeBytes))
}

func (b *bufferWithBackgroundTask) toUnvalidatedReader(off int64) io.ReadCloser {
	return b.decorateReader(b.base.toUnvalidatedReader(off))
}

type chunkReaderWithBackgroundTask struct {
	r    ChunkReader
	task *BackgroundTask
}

func (r *chunkReaderWithBackgroundTask) Read() ([]byte, error) {
	if r.r != nil {
		if chunk, err := r.r.Read(); err != io.EOF {
			return chunk, err
		}
		r.Close()
	}
	if err := r.task.err; err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func (r *chunkReaderWithBackgroundTask) Close() {
	if r.r != nil {
		r.r.Close()
		r.r = nil
		<-r.task.completion
	}
}

type readerWithBackgroundTask struct {
	io.ReadCloser
	task *BackgroundTask
}

func (r *readerWithBackgroundTask) Close() error {
	err := r.ReadCloser.Close()
	<-r.task.completion
	if err != nil {
		return err
	}
	return r.task.err
}
