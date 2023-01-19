package digest_test

import (
	"strconv"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// BenchmarkGenerator measures the hashing throughput for each of the
// digest functions, using varying object sizes.
func BenchmarkGenerator(b *testing.B) {
	for sizeBytes := int64(1); sizeBytes <= 1<<30; sizeBytes *= 2 {
		b.Run(strconv.FormatInt(sizeBytes, 10), func(b *testing.B) {
			buf := make([]byte, sizeBytes)
			for _, digestFunction := range digest.SupportedDigestFunctions {
				b.Run(digestFunction.String(), func(b *testing.B) {
					f, _ := digest.EmptyInstanceName.GetDigestFunction(digestFunction, 0)
					for n := 0; n < b.N; n++ {
						g := f.NewGenerator(sizeBytes)
						g.Write(buf)
						g.Sum()
					}
				})
			}
		})
	}
}
