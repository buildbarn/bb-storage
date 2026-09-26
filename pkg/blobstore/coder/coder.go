package coder

import "github.com/buildbarn/bb-storage/pkg/digest"

// Decoder decodes a value of type U into a value of type T.
type Decoder[T any, U any] interface {
	Decode(val U, d digest.Digest) (T, error)
}

// Encoder encodes a vlaue of type T into a value of type U.
type Encoder[T any, U any] interface {
	Encode(val T, d digest.Digest) (U, error)
}

// Coder is an interface that combines an Encoder and a Decoder.
type Coder[T any, U any] interface {
	Decoder[T, U]
	Encoder[T, U]
}

type joinedCoder[T1, T2, T3 any] struct {
	lhs Coder[T1, T2]
	rhs Coder[T2, T3]
}

// JoinCoders joins two coders together to create a new composite coder
// of their respective types.
func JoinCoders[T1, T2, T3 any](lhs Coder[T1, T2], rhs Coder[T2, T3]) Coder[T1, T3] {
	return &joinedCoder[T1, T2, T3]{lhs: lhs, rhs: rhs}
}

func (c *joinedCoder[T1, T2, T3]) Encode(val T1, d digest.Digest) (T3, error) {
	var zero T3
	intermediate, err := c.lhs.Encode(val, d)
	if err != nil {
		return zero, err
	}
	return c.rhs.Encode(intermediate, d)
}

func (c *joinedCoder[T1, T2, T3]) Decode(val T3, d digest.Digest) (T1, error) {
	var zero T1
	intermediate, err := c.rhs.Decode(val, d)
	if err != nil {
		return zero, err
	}
	return c.lhs.Decode(intermediate, d)
}
