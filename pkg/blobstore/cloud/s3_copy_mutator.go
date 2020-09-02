package cloud

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/buildbarn/bb-storage/pkg/clock"
)

// NewS3LRURefreshingBeforeCopyFunc creates a BeforeCopyFunc which updates the "Used"
// metadata field on s3 objects with the current time.
func NewS3LRURefreshingBeforeCopyFunc(minRefreshAge time.Duration, clock clock.Clock) BeforeCopyFunc {
	return func(asFunc func(interface{}) bool) error {
		var input *s3.CopyObjectInput
		if !asFunc(&input) {
			panic("failed to get CopyObjectInput - not an s3 bucket?")
		}
		now := clock.Now()
		input.CopySourceIfUnmodifiedSince = aws.Time(now.Add(-minRefreshAge))
		input.MetadataDirective = aws.String("REPLACE")
		input.Metadata = aws.StringMap(map[string]string{
			"Used": now.UTC().String(),
		})
		return nil
	}
}
