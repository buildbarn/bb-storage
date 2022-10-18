package buffer

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/protobuf/proto"
)

type backgroundTask struct {
	completion chan struct{}
	err        error
}

type casBufferWithBackgroundTask struct {
	base   Buffer
	digest digest.Digest
	source Source
	task   *backgroundTask
}

// newCASBufferWithBackgroundTask returns a decorated Buffer which will
// at the end of its lifetime wait for the completion of a background
// task. The error of the background task is returned if the Buffer
// itself yields no other error.
//
// This function may be used by implementations of BlobAccess that
// multiplex data, ensuring that the foreground goroutine will at some
// point block, waiting for background synchronizations to complete.
// This ensures that the number of concurrent goroutines remains
// bounded and that any synchronization errors can be propagated.
func newCASBufferWithBackgroundTask(base Buffer, digest digest.Digest, source Source, task func() error) Buffer {
	t := backgroundTask{completion: make(chan struct{})}
	b := &casBufferWithBackgroundTask{
		base:   base,
		digest: digest,
		source: source,
		task:   &t,
	}
	go func() {
		t.err = task()
		close(t.completion)
	}()
	return b
}

func (b *casBufferWithBackgroundTask) decorateBuffer(replacement Buffer) Buffer {
	return &casBufferWithBackgroundTask{
		base: replacement,
		task: b.task,
	}
}

func (b *casBufferWithBackgroundTask) decorateChunkReader(r ChunkReader) ChunkReader {
	return &chunkReaderWithBackgroundTask{
		r:    r,
		task: b.task,
	}
}

func (b *casBufferWithBackgroundTask) decorateReader(r io.ReadCloser) io.ReadCloser {
	return &readerWithBackgroundTask{
		ReadCloser: r,
		task:       b.task,
	}
}

func (b *casBufferWithBackgroundTask) GetSizeBytes() (int64, error) {
	return b.digest.GetSizeBytes(), nil
}

func (b *casBufferWithBackgroundTask) IntoWriter(w io.Writer) error {
	err := b.base.IntoWriter(w)
	<-b.task.completion
	if err != nil {
		return err
	}
	return b.task.err
}

func (b *casBufferWithBackgroundTask) ReadAt(p []byte, off int64) (int, error) {
	n, err := b.base.ReadAt(p, off)
	<-b.task.completion
	if err != nil {
		return n, err
	}
	return n, b.task.err
}

func (b *casBufferWithBackgroundTask) ToProto(m proto.Message, maximumSizeBytes int) (proto.Message, error) {
	mResult, err := b.base.ToProto(m, maximumSizeBytes)
	<-b.task.completion
	if err != nil {
		return nil, err
	}
	return mResult, b.task.err
}

func (b *casBufferWithBackgroundTask) ToByteSlice(maximumSizeBytes int) ([]byte, error) {
	data, err := b.base.ToByteSlice(maximumSizeBytes)
	<-b.task.completion
	if err != nil {
		return nil, err
	}
	return data, b.task.err
}

func (b *casBufferWithBackgroundTask) ToChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	return b.decorateChunkReader(b.base.ToChunkReader(off, maximumChunkSizeBytes))
}

func (b *casBufferWithBackgroundTask) ToReader() io.ReadCloser {
	return b.decorateReader(b.base.ToReader())
}

func (b *casBufferWithBackgroundTask) CloneCopy(maximumSizeBytes int) (Buffer, Buffer) {
	b1, b2 := b.base.CloneCopy(maximumSizeBytes)
	return b.decorateBuffer(b1), b.decorateBuffer(b2)
}

func (b *casBufferWithBackgroundTask) CloneStream() (Buffer, Buffer) {
	b1, b2 := b.base.CloneStream()
	return b.decorateBuffer(b1), b.decorateBuffer(b2)
}

func (b *casBufferWithBackgroundTask) WithTask(task func() error) Buffer {
	return newCASBufferWithBackgroundTask(b, b.digest, b.source, task)
}

func (b *casBufferWithBackgroundTask) Discard() {
	b.base.Discard()
	<-b.task.completion
}

func (b *casBufferWithBackgroundTask) applyErrorHandler(errorHandler ErrorHandler) (Buffer, bool) {
	// For stream-backed buffers, it is not yet known whether they
	// may be read successfully. Wrap the buffer into one that
	// handles I/O errors upon access.
	return newCASErrorHandlingBuffer(b, errorHandler, b.digest, b.source), false
}

func (b *casBufferWithBackgroundTask) toUnvalidatedChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	return b.decorateChunkReader(b.base.toUnvalidatedChunkReader(off, maximumChunkSizeBytes))
}

func (b *casBufferWithBackgroundTask) toUnvalidatedReader(off int64) io.ReadCloser {
	return b.decorateReader(b.base.toUnvalidatedReader(off))
}

type chunkReaderWithBackgroundTask struct {
	r    ChunkReader
	task *backgroundTask
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
	task *backgroundTask
}

func (r *readerWithBackgroundTask) Close() error {
	err := r.ReadCloser.Close()
	<-r.task.completion
	if err != nil {
		return err
	}
	return r.task.err
}
