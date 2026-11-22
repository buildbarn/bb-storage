package zstd

import (
	"context"
	"io"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/semaphore"
)

type boundedPool struct {
	encoderPool sync.Pool
	decoderPool sync.Pool
	encoderSem  *semaphore.Weighted
	decoderSem  *semaphore.Weighted
}

// NewBoundedPool creates a Pool with memory-bounded concurrency. It
// reuses encoders/decoders via sync.Pool and limits concurrent
// operations via semaphores, preventing OOM under high load.
//
// Parameters:
//   - maximumEncoders: Maximum concurrent encoding operations
//   - maximumDecoders: Maximum concurrent decoding operations
//   - encoderOptions: Options passed to zstd.NewWriter
//   - decoderOptions: Options passed to zstd.NewReader
func NewBoundedPool(
	maximumEncoders int64,
	maximumDecoders int64,
	encoderOptions []zstd.EOption,
	decoderOptions []zstd.DOption,
) Pool {
	p := &boundedPool{
		encoderSem: semaphore.NewWeighted(maximumEncoders),
		decoderSem: semaphore.NewWeighted(maximumDecoders),
	}

	p.encoderPool.New = func() interface{} {
		enc, err := zstd.NewWriter(nil, encoderOptions...)
		if err != nil {
			panic("failed to create ZSTD encoder: " + err.Error())
		}
		return enc
	}

	p.decoderPool.New = func() interface{} {
		dec, err := zstd.NewReader(nil, decoderOptions...)
		if err != nil {
			panic("failed to create ZSTD decoder: " + err.Error())
		}
		return dec
	}

	return p
}

func (p *boundedPool) NewEncoder(ctx context.Context, w io.Writer) (Encoder, error) {
	if err := util.AcquireSemaphore(ctx, p.encoderSem, 1); err != nil {
		return nil, err
	}

	enc := p.encoderPool.Get().(*zstd.Encoder)
	enc.Reset(w)
	return &pooledEncoder{Encoder: enc, pool: p}, nil
}

func (p *boundedPool) NewDecoder(ctx context.Context, r io.Reader) (Decoder, error) {
	if err := util.AcquireSemaphore(ctx, p.decoderSem, 1); err != nil {
		return nil, err
	}

	dec := p.decoderPool.Get().(*zstd.Decoder)
	if err := dec.Reset(r); err != nil {
		p.decoderSem.Release(1)
		return nil, err
	}
	return &pooledDecoder{Decoder: dec, pool: p}, nil
}

func (p *boundedPool) releaseEncoder(e *pooledEncoder) {
	if e.Encoder == nil {
		return
	}
	e.Encoder.Reset(nil)
	p.encoderPool.Put(e.Encoder)
	e.Encoder = nil
	p.encoderSem.Release(1)
}

func (p *boundedPool) releaseDecoder(d *pooledDecoder) {
	if d.Decoder == nil {
		return
	}
	_ = d.Decoder.Reset(nil)
	p.decoderPool.Put(d.Decoder)
	d.Decoder = nil
	p.decoderSem.Release(1)
}

type pooledEncoder struct {
	*zstd.Encoder
	pool *boundedPool
}

func (e *pooledEncoder) Close() error {
	if e.Encoder == nil {
		return nil
	}
	err := e.Encoder.Close()
	e.pool.releaseEncoder(e)
	return err
}

type pooledDecoder struct {
	*zstd.Decoder
	pool *boundedPool
}

func (d *pooledDecoder) Close() {
	if d.Decoder == nil {
		return
	}
	d.pool.releaseDecoder(d)
}
