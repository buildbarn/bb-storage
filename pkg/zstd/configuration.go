package zstd

import (
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/zstd"
	"github.com/klauspost/compress/zstd"
)

// NewPoolFromConfiguration creates a Pool from a protobuf configuration
// message. If the configuration is nil, an unbounded pool with default
// options is returned.
func NewPoolFromConfiguration(config *configuration.PoolConfiguration) Pool {
	if config == nil {
		return NewUnboundedPool(
			[]zstd.EOption{zstd.WithEncoderConcurrency(1)},
			[]zstd.DOption{zstd.WithDecoderConcurrency(1)},
		)
	}

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

	return NewBoundedPool(
		config.MaximumEncoders,
		config.MaximumDecoders,
		encoderOptions,
		decoderOptions,
	)
}
