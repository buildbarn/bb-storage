package zstd

import (
	"github.com/buildbarn/bb-storage/pkg/clock"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/zstd"
	"github.com/klauspost/compress/zstd"
)

// NewPoolFromConfiguration creates a Pool from a protobuf configuration
// message. If the configuration is nil, an unbounded pool with default
// options is returned. The returned pool is always wrapped with metrics.
func NewPoolFromConfiguration(config *configuration.PoolConfiguration) Pool {
	var pool Pool
	if config == nil {
		pool = NewUnboundedPool(
			[]zstd.EOption{zstd.WithEncoderConcurrency(1)},
			[]zstd.DOption{zstd.WithDecoderConcurrency(1)},
		)
	} else {
		encoderOptions := []zstd.EOption{
			zstd.WithEncoderConcurrency(1),
		}
		if config.EncoderWindowSizeBytes != 0 {
			encoderOptions = append(encoderOptions, zstd.WithWindowSize(int(config.EncoderWindowSizeBytes)))
		}
		if config.EncoderLevel != 0 {
			encoderOptions = append(encoderOptions, zstd.WithEncoderLevel(zstd.EncoderLevel(config.EncoderLevel)))
		}

		decoderOptions := []zstd.DOption{
			zstd.WithDecoderConcurrency(1),
		}
		if config.DecoderWindowSizeBytes != 0 {
			decoderOptions = append(decoderOptions, zstd.WithDecoderMaxWindow(uint64(config.DecoderWindowSizeBytes)))
		}

		pool = NewBoundedPool(
			config.MaximumEncoders,
			config.MaximumDecoders,
			encoderOptions,
			decoderOptions,
		)
	}

	return NewMetricsPool(pool, clock.SystemClock, "ZstdPool")
}
