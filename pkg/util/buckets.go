package util

import (
	"fmt"
	"math"
	"strconv"
)

func getBucketBoundary(significand string, exponent int) float64 {
	v, err := strconv.ParseFloat(fmt.Sprintf("%se%d", significand, exponent), 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to compute bucket boundary: %s", err))
	}
	return v
}

// DecimalExponentialBuckets generates a series of exponential bucket
// boundaries that can be used for Prometheus histogram objects. Instead
// of using powers of 2, this function uses 10^(1/m) as the exponent.
// This has the advantage of yielding round numbers at every power of
// ten.
//
// Instead of computing the actual value of 10^(n/m), we first compute
// values that are up to five digits accurate within a single power of
// 10. This is done to ensure that the metric label name remains short
// and unaffected by the precision of math libraries and hardware
// floating point units.
//
// Under the hood, strconv.ParseFloat() is used. Unlike math.Pow(), it
// ensures that the resulting floating point value is the one that
// yields the desired shortest decimal representation.
func DecimalExponentialBuckets(lowestPowerOf10, powersOf10, stepsInBetween int) []float64 {
	// Compute boundaries within a single power of 10.
	boundaries := make([]string, 0, stepsInBetween+1)
	for i := 0; i <= stepsInBetween; i++ {
		boundaries = append(
			boundaries,
			fmt.Sprintf("%f", math.Pow(10.0, float64(i)/float64(stepsInBetween+1)))[:6])
	}

	// Extend to all powers of 10 that are requested.
	buckets := make([]float64, 0, powersOf10*len(boundaries)+1)
	for i := 0; i < powersOf10; i++ {
		for _, boundary := range boundaries {
			buckets = append(buckets, getBucketBoundary(boundary, lowestPowerOf10+i))
		}
	}
	return append(buckets, getBucketBoundary("1", lowestPowerOf10+powersOf10))
}
