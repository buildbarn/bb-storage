package util_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestDecimalExponentialBuckets(t *testing.T) {
	// Unlike with prometheus.ExponentialBuckets, floating point
	// imprecision should not accumulate. In the general case, every
	// power of ten should be represented accurately.
	require.Equal(t, util.DecimalExponentialBuckets(-9, 18, 0), []float64{
		1e-09, 1e-08, 1e-07, 1e-06, 1e-05, 1e-04, 1e-03, 1e-02, 1e-01,
		1e+00, 1e+01, 1e+02, 1e+03, 1e+04, 1e+05, 1e+06, 1e+07, 1e+08,
		1e+09,
	})
	require.Equal(t, util.DecimalExponentialBuckets(-9, 18, 1), []float64{
		1e-09, 3.1622e-09, 1e-08, 3.1622e-08, 1e-07, 3.1622e-07,
		1e-06, 3.1622e-06, 1e-05, 3.1622e-05, 1e-04, 3.1622e-04,
		1e-03, 3.1622e-03, 1e-02, 3.1622e-02, 1e-01, 3.1622e-01,
		1e+00, 3.1622e+00, 1e+01, 3.1622e+01, 1e+02, 3.1622e+02,
		1e+03, 3.1622e+03, 1e+04, 3.1622e+04, 1e+05, 3.1622e+05,
		1e+06, 3.1622e+06, 1e+07, 3.1622e+07, 1e+08, 3.1622e+08,
		1e+09,
	})
	require.Equal(t, util.DecimalExponentialBuckets(-9, 18, 2), []float64{
		1e-09, 2.1544e-09, 4.6415e-09, 1e-08, 2.1544e-08, 4.6415e-08,
		1e-07, 2.1544e-07, 4.6415e-07, 1e-06, 2.1544e-06, 4.6415e-06,
		1e-05, 2.1544e-05, 4.6415e-05, 1e-04, 2.1544e-04, 4.6415e-04,
		1e-03, 2.1544e-03, 4.6415e-03, 1e-02, 2.1544e-02, 4.6415e-02,
		1e-01, 2.1544e-01, 4.6415e-01, 1e+00, 2.1544e+00, 4.6415e+00,
		1e+01, 2.1544e+01, 4.6415e+01, 1e+02, 2.1544e+02, 4.6415e+02,
		1e+03, 2.1544e+03, 4.6415e+03, 1e+04, 2.1544e+04, 4.6415e+04,
		1e+05, 2.1544e+05, 4.6415e+05, 1e+06, 2.1544e+06, 4.6415e+06,
		1e+07, 2.1544e+07, 4.6415e+07, 1e+08, 2.1544e+08, 4.6415e+08,
		1e+09,
	})
}
