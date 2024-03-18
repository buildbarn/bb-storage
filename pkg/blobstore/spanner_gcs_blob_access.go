package blobstore

//
// BuildBarn blob access layer that stores metadata and small (<10MB) blobs in Spanner, while
// storing large blobs in a GCS bucket.  We rely on Google to delete stale objects, but we avoid
// the situation where GCS blobs are deleted before the Spanner records referencing them.  We
// Update reference timestamps at insert time (in Put) and in FindMissing (to ensure blobs live
// long enough for a bazel build or test to complete).  We also prevent rows scheduled for
// deletion in GCS from being returned by Get.  Thus, if a blob is stale in GCS, it's metadata
// should also be stale in Spanner, as long as the GCS TTL is at least as long as the Spanner TTL.
// To make sure action cache entries are retained on an LRU-like basis, we queue up a list of
// hashes as they are referenced and periodically update ther reference time.  Doing this one at
// a time incurs too much overhead in spanner, so we perform bulk operations.  The downside of
// this approach is that we can lose reference updates if the servers reboot while updates are
// still queued.  Worst case, these objects will be evicted and need to be rebuilt the next time
// they are needed.
//
// The LRU-like algorithm is intended to further reduce the frequency of object updates.  It has
// two arenas: one of objects that will expire in the configured timeframe, and one of objects
// that have been extended by having their reference times updated to the latest time they were
// referenced, but only when they have remained in the cache for half of their configured lifetimes.
// This removes the need to update young objects that are accessed multiple times when they first
// are entered into the cache.  Similarly, when an object's reference time is updated, it will
// not receive further updates until it has spent an additional amount of time in the cache equal
// to half of the configured lifetime.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/api/iterator"
        "google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/storage"
	dbpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

const (
	maxSize           int64 = 10*1024*1024 - 1
	tableName               = "Blobs_v1_0"
	maxRefBulkSz            = 600 // Maximum number of hashes to gather before doing a bulk reftime update
	maxRefHours             = 1   // Maximum time to wait before updating reference times
	defaultDaysToLive       = 7

	// Labels for backend metrics
	BE_SPANNER = "SPANNER"
	BE_GCS     = "GCS"

	// Operations on blobs
	BE_GET   = "GET"
	BE_DEL   = "DEL"
	BE_PUT   = "PUT"
	BE_FM    = "FINDMISSING"
	BE_TOUCH = "TOUCH"

	// Blob storage locations.
	// A bitmask makes it easier to support GCS-FUSE when we write small blobs everywhere.
	LOC_SPANNER = 0x01
	LOC_GCS	    = 0x02
)

var (
	spannerGCSBlobAccessPrometheusMetrics sync.Once

	spannerMalformedKeyCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "spanner_malformed_key_total",
			Help:      "Number of keys that can't be parsed properly (should always be 0)",
		})
	gcsReftimeUpdateCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "gcs_reftime_update_total",
			Help:      "Number of GCS object reference times updates have been attempted",
		})
	gcsReftimeUpdateFailedCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "gcs_reftime_update_failed_total",
			Help:      "Number of GCS object reference times updates failed",
		})
	spannerReftimeUpdateCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "spanner_reftime_update_total",
			Help:      "Number of spanner object reference times updates have been attempted",
		})
	spannerReftimeUpdateFailedCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "spanner_reftime_update_failed_total",
			Help:      "Number of spanner object reference times updates failed",
		})
	spannerExpiredBlobReadIgnoredCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "spanner_expired_blob_read_ignored_total",
			Help:      "Number of ignored read attempts of expired spanner blobs",
		})
	gcsFailedReadDeletedBlobCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "gcs_failed_read_deleted_blob_total",
			Help:      "Number of deletes of GCS blobs that couldn't be read",
		})
	gcsFailedReadDeleteBlobFailedCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "gcs_failed_read_delete_blob_failed_total",
			Help:      "Number of failed deletes of GCS blobs that couldn't be read",
		})
	gcsPutFailedContextCanceledCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "gcs_put_failed_context_canceled_total",
			Help:      "Number of failed puts of GCS blobs because the context was canceled",
		})
	gcsPutFailedDeadlineExceededCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "gcs_put_failed_deadline_exceeded_total",
			Help:      "Number of failed puts of GCS blobs because the deadline was exceeded",
		})
	gcsPutFailedOtherCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "gcs_put_failed_other_total",
			Help:      "Number of failed puts of GCS blobs because of other reasons",
		})
	spannerMalformedBlobDeletedCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "spanner_malformed_blob_deleted_total",
			Help:      "Number of malformed blobs that were deleted",
		},
		[]string{"backend_type"})
	spannerMalformedBlobDeleteFailedCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "spanner_malformed_blob_delete_failed_total",
			Help:      "Number of malformed blobs that could not be deleted",
		},
		[]string{"backend_type"})
	backendOperationsDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "backend_operations_duration_seconds",
			Help:      "Amount of time spent per backend operation in seconds.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"storage_type", "backend_type", "operation"})
)

type spannerGCSBlobAccess struct {
	capabilities.Provider

	spannerClient *spanner.Client
	gcsBucket     *storage.BucketHandle

	readBufferFactory ReadBufferFactory
	storageType       string
	daysToLive        uint64        // to avoid converting back and forth
	expirationAge     time.Duration // same as above, but easier for time calculations
	refUpdateThresh   time.Duration // when we start updating ReferenceTime
	refChan           chan keyLoc
}

type spannerRecord struct {
	Key           string
	InlineData    []byte
	ReferenceTime time.Time
}

// Keep track of keys and the storage locations where they reside.
type keyLoc struct {
	key	string
	loc	int
}

// databaseName is of the form "projects/<project ID>/instances/<instance name>/databases/<database name>".
// tableName should include the name of the table and the version of the table's schema.
func createSpannerTable(ctx context.Context, databaseName string, daysToLive uint64) error {
	ac, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		log.Printf("Can't create spanner database admin client: %v", err)
		return err
	}
	defer ac.Close()

	// If daysToLive is zero, use the default.
	if daysToLive == 0 {
		daysToLive = defaultDaysToLive
	}

	s := `CREATE TABLE ` + tableName + ` (
		Key STRING(MAX),
		InlineData BYTES(MAX),
		ReferenceTime TIMESTAMP NOT NULL,
	) PRIMARY KEY(Key), ROW DELETION POLICY (OLDER_THAN(ReferenceTime, INTERVAL ` + strconv.FormatUint(daysToLive, 10) + ` DAY))`
	op, err := ac.UpdateDatabaseDdl(ctx, &dbpb.UpdateDatabaseDdlRequest{
		Database: databaseName,
		Statements: []string{
			s,
		},
	})
	if err != nil {
		return err
	}
	if err = op.Wait(ctx); err != nil {
		return err
	}
	return nil
}

func getSpannerTTL(ctx context.Context, databaseName string) (uint64, error) {
	ac, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		log.Printf("Can't create spanner database admin client: %v", err)
		return 0, err
	}
	defer ac.Close()
	resp, err := ac.GetDatabaseDdl(ctx, &dbpb.GetDatabaseDdlRequest{
		Database: databaseName,
	})
	if err != nil {
		log.Printf("Can't read spanner database DDL: %v", err)
		return 0, err
	}
	for _, s := range resp.Statements {
		if strings.Contains(s, "ROW DELETION POLICY") {
			if i := strings.Index(s, "INTERVAL"); i != -1 {
				var days uint64
				n, err := fmt.Sscanf(s[i:], "INTERVAL %d DAY", &days)
				if err == nil && n != 0 {
					return days, nil
				}
			}
		}
	}
	return 0, nil
}

func updateSpannerDeletionPolicy(ctx context.Context, databaseName string, days uint64) error {
	ac, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		log.Printf("Can't create spanner database admin client: %v", err)
		return err
	}
	defer ac.Close()
	s := `ALTER TABLE ` + tableName + ` REPLACE ROW DELETION POLICY (OLDER_THAN(ReferenceTime, INTERVAL ` + strconv.FormatUint(days, 10) + ` DAY))`
	op, err := ac.UpdateDatabaseDdl(ctx, &dbpb.UpdateDatabaseDdlRequest{
		Database: databaseName,
		Statements: []string{
			s,
		},
	})
	if err != nil {
		return err
	}
	if err = op.Wait(ctx); err != nil {
		return err
	}
	return nil
}

func createGCSBucket(ctx context.Context, gcsBucket *storage.BucketHandle, databaseName string, daysToLive uint64) error {
	// Set up the deletion policy.
	var attrs *storage.BucketAttrs

	// If daysToLive is zero, use the default.
	if daysToLive == 0 {
		daysToLive = defaultDaysToLive
	}

	lifecycle := storage.Lifecycle{}
	lifecycle.Rules = append(lifecycle.Rules, storage.LifecycleRule{
		Action: storage.LifecycleAction{
			Type: storage.DeleteAction,
		},
		Condition: storage.LifecycleCondition{
			DaysSinceCustomTime: int64(daysToLive),
		},
	})
	attrs = &storage.BucketAttrs{
		Lifecycle: lifecycle,
	}

	// Extract the project ID from the spanner database name (no, really).
	s := strings.Split(databaseName, "/")
	projectID := s[1]

	// Create the bucket.
	if err := gcsBucket.Create(ctx, projectID, attrs); err != nil {
		return err
	}
	return nil
}

func getGCSTTL(ctx context.Context, gcsBucket *storage.BucketHandle) (uint64, error) {
	attrs, err := gcsBucket.Attrs(ctx)
	if err != nil {
		return 0, err
	}
	for _, r := range attrs.Lifecycle.Rules {
		if r.Action.Type == storage.DeleteAction && r.Condition.DaysSinceCustomTime != 0 {
			return uint64(r.Condition.DaysSinceCustomTime), nil
		}
	}
	return 0, nil
}

func updateGCSDeletionPolicy(ctx context.Context, gcsBucket *storage.BucketHandle, daysToLive uint64) error {
	lifecycle := storage.Lifecycle{}
	lifecycle.Rules = append(lifecycle.Rules, storage.LifecycleRule{
		Action: storage.LifecycleAction{
			Type: storage.DeleteAction,
		},
		Condition: storage.LifecycleCondition{
			DaysSinceCustomTime: int64(daysToLive),
		},
	})
	attrs := storage.BucketAttrsToUpdate{
		Lifecycle: &lifecycle,
	}
	_, err := gcsBucket.Update(ctx, attrs)
	return err
}

// Convert a digest to the key of the entry in the Spanner database and GCS bucket.
func (ba *spannerGCSBlobAccess) digestToKey(digest digest.Digest) string {
	sz := strconv.FormatInt(digest.GetSizeBytes(), 10)
	if ba.storageType == "AC" {
		// Instance names are hierarchical, but '/' has special meaning for GCS.
		// We don't need a hierarchical namespace for storage.
		instance := strings.ReplaceAll(digest.GetInstanceName().String(), "/", "-")
		return digest.GetHashString() + "-" + sz + "-" + instance
	} else if ba.storageType == "CAS" {
		return digest.GetHashString() + "-" + sz
	} else {
		panic("Invalid Spanner storage Type configured")
	}
}

func (ba *spannerGCSBlobAccess) findLocFromDigest(digest digest.Digest) int {
	var loc int
	if ba.storageType == "AC" {
		panic("Can't call fileLocFromDigest for Action Cache")
	}
	sz := digest.GetSizeBytes()
	if sz > maxSize {
		loc |= LOC_GCS
	} else {
		loc |= LOC_SPANNER
	}
	return loc
}

func (ba *spannerGCSBlobAccess) findLocFromKey(key string) (int, error) {
	if ba.storageType == "AC" {
		panic("Can't call fileLocFromKey for Action Cache")
	}
	s := strings.Split(key, "-")
	sz, err := strconv.ParseInt(s[1], 10, 64)
	if err != nil {
		return 0, err
	}
	var loc int
	if sz > maxSize {
		loc |= LOC_GCS
	} else {
		loc |= LOC_SPANNER
	}
	return loc, nil
}

// NewSpannerGCSBlobAccess creates a BlobAccess that uses Spanner and GCS as its backing store.
func NewSpannerGCSBlobAccess(databaseName string, gcsBucketName string, readBufferFactory ReadBufferFactory, storageType string, daysToLive uint64, capabilitiesProvider capabilities.Provider, clientOpts []option.ClientOption) (BlobAccess, error) {
	storageType = strings.ToUpper(storageType)

	// If daysToLive is zero, use the default.
	if daysToLive == 0 {
		daysToLive = defaultDaysToLive
	}
	s := strconv.FormatUint(24*daysToLive, 10) + "h"
	expirationAge, err := time.ParseDuration(s)
	if err != nil {
		return nil, fmt.Errorf("Invalid expirationDays value configured: %v", err)
	}
	s = strconv.FormatUint(12*daysToLive, 10) + "h" // half of the expiration age
	refUpdateThresh, _ := time.ParseDuration(s)

	spannerGCSBlobAccessPrometheusMetrics.Do(func() {
		prometheus.MustRegister(spannerMalformedKeyCount)
		prometheus.MustRegister(gcsReftimeUpdateCount)
		prometheus.MustRegister(gcsReftimeUpdateFailedCount)
		prometheus.MustRegister(spannerReftimeUpdateCount)
		prometheus.MustRegister(spannerReftimeUpdateFailedCount)
		prometheus.MustRegister(spannerMalformedBlobDeletedCount)
		prometheus.MustRegister(spannerMalformedBlobDeleteFailedCount)
		prometheus.MustRegister(spannerExpiredBlobReadIgnoredCount)
		prometheus.MustRegister(gcsFailedReadDeletedBlobCount)
		prometheus.MustRegister(gcsFailedReadDeleteBlobFailedCount)
		prometheus.MustRegister(gcsPutFailedContextCanceledCount)
		prometheus.MustRegister(gcsPutFailedDeadlineExceededCount)
		prometheus.MustRegister(gcsPutFailedOtherCount)
		prometheus.MustRegister(backendOperationsDurationSeconds)
	})

	ctx := context.Background()
	spannerClient, err := spanner.NewClient(ctx, databaseName)
	if err != nil {
		log.Printf("Can't create spanner client: %v", err)
		return nil, err
	}

	// If the spanner table doesn't exist, create it.
	stmt := spanner.NewStatement(`SELECT 1 FROM information_schema.tables WHERE table_name = "` + tableName + `"`)
	iter := spannerClient.Single().Query(ctx, stmt)
	_, err = iter.Next()
	iter.Stop()
	log.Printf("spanner table check, err = %v", err)
	if err == iterator.Done {
		// Table doesn't exist.  Create it.
		if err = createSpannerTable(ctx, databaseName, daysToLive); err != nil {
			// We could have raced with another pod.  Check if the table exists.
			iter := spannerClient.Single().Query(ctx, stmt)
			_, xerr := iter.Next()
			iter.Stop()
			if xerr == iterator.Done {
				// Table still doesn't exist.
				spannerClient.Close()
				log.Printf("Can't create spanner table: %v", err)
				return nil, err
			}
		}
	} else if err == nil {
		// Check if we need to update the TTL.
		days, err := getSpannerTTL(ctx, databaseName)
		if err != nil {
			log.Printf("Can't determine Spanner TTL: %v", err)
		}
		if days != daysToLive {
			err = updateSpannerDeletionPolicy(ctx, databaseName, daysToLive)
			if err != nil {
				log.Printf("Can't update Spanner TTL: %v", err)
			} else {
				log.Printf("Spanner TTL changed from %d to %d days", days, daysToLive)
			}
		}
	}

	storageClient, err := storage.NewClient(ctx, clientOpts...)
	if err != nil {
		spannerClient.Close()
		log.Printf("Can't create GCS client: %v", err)
		return nil, err
	}
	gcsBucket := storageClient.Bucket(gcsBucketName)

	// If the GCS bucket doesn't exist, create it.
	_, err = gcsBucket.Attrs(ctx)
	if err == storage.ErrBucketNotExist {
		err = createGCSBucket(ctx, gcsBucket, databaseName, daysToLive)
		if err != nil {
			// We could have raced with another pod.  Check if the bucket exists.
			_, xerr := gcsBucket.Attrs(ctx)
			if xerr == storage.ErrBucketNotExist {
				// Bucket still doesn't exist.
				spannerClient.Close()
				storageClient.Close()
				log.Printf("Can't create GCS bucket: %v", err)
				return nil, err
			}
		}
	} else if err != nil {
		spannerClient.Close()
		storageClient.Close()
		log.Printf("Can't access GCS bucket: %v", err)
		return nil, err
	} else {
		// Check if we need to update the TTL.
		days, err := getGCSTTL(ctx, gcsBucket)
		if err != nil {
			log.Printf("Can't determine GCS TTL: %v", err)
		}
		if days != daysToLive {
			err = updateGCSDeletionPolicy(ctx, gcsBucket, daysToLive)
			if err != nil {
				log.Printf("Can't update GCS TTL: %v", err)
			} else {
				log.Printf("GCS TTL changed from %d to %d days", days, daysToLive)
			}
		}
	}

	log.Printf("NewSpannerGCSBlobAccess type %s", storageType)

	// FindMissing takes care of updaing the reference time on CAS objects, but we'd like to update
	// AC objects when they're read, to simulate an LRU cache.  Doing this one at a time is inefficient.
	var refCh chan keyLoc
	if storageType == "AC" {
		refCh = make(chan keyLoc, maxRefBulkSz)
	}

	ba := &spannerGCSBlobAccess{
		Provider:      capabilitiesProvider,
		spannerClient: spannerClient,
		gcsBucket:     gcsBucket,

		readBufferFactory: readBufferFactory,
		storageType:       storageType,
		daysToLive:        daysToLive,
		expirationAge:     expirationAge,
		refUpdateThresh:   refUpdateThresh,
		refChan:           refCh,
	}
	if refCh != nil {
		go ba.bulkUpdate(refCh)
	}
	return ba, nil
}

func (ba *spannerGCSBlobAccess) delete(ctx context.Context, key string, loc int) error {
	deleteMut := spanner.Delete(tableName, spanner.Key{key})
	start := time.Now()
	_, err := ba.spannerClient.Apply(ctx, []*spanner.Mutation{deleteMut})
	backendOperationsDurationSeconds.WithLabelValues(ba.storageType, BE_SPANNER, BE_DEL).Observe(time.Now().Sub(start).Seconds())
	if err != nil {
		return err
	}

	// Now if it was also in GCS, delete it there
	if (loc & LOC_GCS) != 0 {
		object := ba.gcsBucket.Object(key)
		start := time.Now()
		err = object.Delete(ctx)
		backendOperationsDurationSeconds.WithLabelValues(ba.storageType, BE_GCS, BE_DEL).Observe(time.Now().Sub(start).Seconds())
		return err
	}

	return nil
}

func (ba *spannerGCSBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	//log.Printf("SpannerGCSBlobAccess type %#+v GET digest %s", ba.storageType, digest)
	if err := util.StatusFromContext(ctx); err != nil {
		return buffer.NewBufferFromError(err)
	}
	key := ba.digestToKey(digest)
	//log.Printf("SpannerGCSBlobAccess GET key is %s", key)

	// Grab the row itself
	now := time.Now().UTC()
	row, err := ba.spannerClient.Single().ReadRow(ctx, tableName, spanner.Key{key}, []string{"InlineData", "ReferenceTime"})
	backendOperationsDurationSeconds.WithLabelValues(ba.storageType, BE_SPANNER, BE_GET).Observe(time.Now().Sub(now).Seconds())
	if err != nil {
		log.Printf("GET error: ReadRow key %s failed: %v", key, err)
		return buffer.NewBufferFromError(err)
	}

	var s struct {
		InlineData    []byte
		ReferenceTime time.Time
	}
	err = row.ToStruct(&s)
	if err != nil {
		log.Printf("GET error: ToStruct key %s failed: %v", key, err)
		return buffer.NewBufferFromError(err)
	}

	var loc int
	if len(s.InlineData) == 0 {
		loc |= LOC_GCS
	} else {
		loc |= LOC_SPANNER
	}

	// Exclude expired blobs -- they don't exist anymore; we're waiting for the storage to delete them.
	if !now.Before(s.ReferenceTime.Add(ba.expirationAge)) {
		spannerExpiredBlobReadIgnoredCount.Inc()
		log.Printf("Ignoring stale key %s", key)
		return buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found"))
	}

	validationFunc := func(dataIsValid bool) {
		if !dataIsValid {
			var beType string
			if (loc & LOC_GCS) != 0 {
				beType = BE_GCS
			} else {
				beType = BE_SPANNER
			}
			if err := ba.delete(ctx, key, loc); err == nil {
				spannerMalformedBlobDeletedCount.WithLabelValues(beType).Inc()
				log.Printf("Blob %s was malformed and has been deleted from Spanner/GCS successfully", digest.String())
			} else {
				spannerMalformedBlobDeleteFailedCount.WithLabelValues(beType).Inc()
				log.Printf("Blob %s was malformed and could not be deleted from Spanner/GCS: %v", digest.String(), err)
			}
		}
	}

	var b buffer.Buffer
	if (loc & LOC_GCS) != 0 {
		// We gotta go get it from GCS
		obj := ba.gcsBucket.Object(key)

		r, err := obj.NewReader(ctx)
		if err != nil {
			// If we couldn't read the bucket, then let's delete it from spanner (and from gcs if we can!)
			if err2 := ba.delete(ctx, key, loc); err2 == nil {
				gcsFailedReadDeletedBlobCount.Inc()
				log.Printf("Blob %s was inaccessible in GCS (due to %v) and has been deleted from Spanner/GCS successfully", digest.String(), err)
			} else {
				gcsFailedReadDeleteBlobFailedCount.Inc()
				log.Printf("Blob %s was inaccessible in GCS (due to %v) and could not be deleted from Spanner/GCS: %s", digest.String(), err, err2)
			}
			return buffer.NewBufferFromError(err)
		}
		b = ba.readBufferFactory.NewBufferFromReader(digest, r, validationFunc)
		b = buffer.WithErrorHandler(
			b,
			&spannerGCSErrorHandler{
				start:  time.Now(),
				sType:  ba.storageType,
				beType: BE_GCS,
				op:     BE_GET,
			})
	} else {
		// It's inline, so return it directly
		b = ba.readBufferFactory.NewBufferFromByteSlice(digest, s.InlineData, validationFunc)
	}
	_, err = b.GetSizeBytes()
	if err != nil {
		log.Printf("GET ERROR: key %s, type %v: %v", key, ba.storageType, err)
	}
	if ba.refChan != nil && now.After(s.ReferenceTime.Add(ba.refUpdateThresh)) {
		log.Printf("GET: scheduling touch reftime for key %s reftime %s", key, s.ReferenceTime)
		ba.refChan <- keyLoc{key: key, loc: loc}
	}
	return b
}

func (ba *spannerGCSBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	//log.Printf("SpannerGCSBlobAccess type %#+v PUT digest %s", ba.storageType, digest)
	if err := util.StatusFromContext(ctx); err != nil {
		b.Discard()
		return err
	}

	// If we're bigger than 10MB, we have to offload to GCS
	size, err := b.GetSizeBytes()
	if err != nil {
		log.Printf("Put Blob %s: can't get size: %v", digest, err)
		b.Discard()
		return err
	}

	key := ba.digestToKey(digest)
	//log.Printf("SpannerGCSBlobAccess PUT key is %s, size %d", key, size)

	var inlineData []byte = nil
	now := time.Now().UTC()
	if size > maxSize {
		obj := ba.gcsBucket.Object(key)
		w := obj.NewWriter(ctx)
		start := time.Now()
		var err error
		if _, err = io.Copy(w, b.ToReader()); err == nil {
			if err = w.Close(); err != nil {
				log.Printf("Blob %s can't be copied to GCS, close failed: %v", digest, err)
			}
		} else {
			log.Printf("Blob %s can't be copied to GCS, write failed: %v", digest, err)
		}
		backendOperationsDurationSeconds.WithLabelValues(ba.storageType, BE_GCS, BE_PUT).Observe(time.Now().Sub(start).Seconds())
		if err != nil {
			if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
				gcsPutFailedContextCanceledCount.Inc()
			} else if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
				gcsPutFailedDeadlineExceededCount.Inc()
			} else {
				gcsPutFailedOtherCount.Inc()
			}
			return err
		}
		inlineData = nil
		ba.touchGCSObject(ctx, key, now)
	} else {
		inlineData, err = b.ToByteSlice(int(maxSize))
		if err != nil {
			log.Printf("Blob %s can't be copied to Spanner: %v", digest, err)
			return err
		}
		if len(inlineData) == 0 {
			log.Printf("WARNING: ByteSlice size is 0, expected %d, key %s", size, key)
		}
	}

	// Now insert into the spanners no matter what!
	rec := spannerRecord{
		Key:           key,
		InlineData:    inlineData,
		ReferenceTime: now,
	}
	insertMut, err := spanner.ReplaceStruct(tableName, rec)
	if err != nil {
		log.Printf("Can't create mutation for Blob %s: %v", digest, err)
		return err
	}

	start := time.Now()
	_, err = ba.spannerClient.Apply(ctx, []*spanner.Mutation{insertMut})
	backendOperationsDurationSeconds.WithLabelValues(ba.storageType, BE_SPANNER, BE_PUT).Observe(time.Now().Sub(start).Seconds())
	if err != nil {
		log.Printf("Can'apply create mutation for Blob %s: %v", digest, err)
		return err
	}
	return nil
}

// This function is only supported for CAS objects.
func (ba *spannerGCSBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	if err := util.StatusFromContext(ctx); err != nil {
		return digest.EmptySet, err
	}
	if digests.Empty() {
		return digest.EmptySet, nil
	}
	// This funciton isn't supported for the action cache.
	if ba.storageType == "CAS" {
		return digest.EmptySet, status.Error(codes.Unimplemented, "Bazel action cache does not support bulk existence checking")
	}

	keyToDigest := make(map[string]digest.Digest, digests.Length()) // Needed for timestamp updates
	ksl := make([]string, digests.Length())                         // Needed for the query
	for _, digest := range digests.Items() {
		k := ba.digestToKey(digest)
		//log.Printf("FINDMISSING digest %s key %s", digest, k)
		keyToDigest[k] = digest
		ksl = append(ksl, k)
	}

	// We want to grab anything not in the Blobs table.  First find what's there so we can exclude them from the list
	// of missing blobs.  Then decide which of the existing ones need their reftime to be updated.
	stmt := spanner.NewStatement(`SELECT Key, ReferenceTime FROM ` + tableName + ` where Key IN UNNEST(@keys) and TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), ReferenceTime, DAY) < @expdays`)
	stmt.Params["keys"] = ksl
	stmt.Params["expdays"] = int64(ba.daysToLive)
	start := time.Now()
	iter := ba.spannerClient.Single().Query(ctx, stmt)
	backendOperationsDurationSeconds.WithLabelValues("CAS", BE_SPANNER, BE_FM).Observe(time.Now().Sub(start).Seconds())

	missing := digest.NewSetBuilder()
	keyToRefTime := make(map[string]time.Time, digests.Length())
	err := iter.Do(func(row *spanner.Row) error {
		// Errors in this function (interpretting the row results) should only occur if someone changes the
		// schema without updating this file.
		var key string
		var refTime time.Time
		err := row.Column(0, &key)
		if err != nil {
			log.Printf("ERROR Column 0 wanted Key, got %v", err)
		}
		if key == "" {
			return nil
		}
		err = row.Column(1, &refTime)
		if err != nil {
			log.Printf("ERROR Column 1 wanted ReferenceTime, got %v", err)
		}
		//log.Printf("FOUND key %s, digest %s", key, keyToDigest[key])
		keyToRefTime[key] = refTime
		delete(keyToDigest, key)
		return nil
	})

	if err != nil {
		return digest.EmptySet, err
	}

	// Now keyToDigest consists only of missing blobs.  Prepare the missing digest set to return to the caller.
	for _, digest := range keyToDigest {
		missing.Add(digest)
	}

	// Now update the CustomTime metadata attribute for the GCS Blobs we have, and the ReferenceTime field for the
	// Spanner blobs we have.  GCS blobs also have records in spanner to make FindMissing efficient.
	now := time.Now().UTC()
	keys := make([]string, 0, digests.Length())
	for key, refTime := range keyToRefTime {
		if now.After(refTime.Add(ba.refUpdateThresh)) {
			log.Printf("FINDMISSING: scheduling touch reftime for key %s reftime %s", key, refTime)
			loc, err := ba.findLocFromKey(key)
			if err != nil {
				spannerMalformedKeyCount.Inc()
				log.Printf("Couldn't extract location from key %s", key)
			}
			if (loc & LOC_GCS) != 0 {
				ba.touchGCSObject(context.Background(), key, now)
			}
			keys = append(keys, key)
		}
	}
	if len(keys) != 0 {
		ba.touchSpannerObjects(context.Background(), keys, now)
	}

	return missing.Build(), nil
}

func (ba *spannerGCSBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	b, _ := slicer.Slice(ba.Get(ctx, parentDigest), childDigest)
	return b
}

// Update the CustomTime metadata attribute for the GCS blob.  We wouldn't want the blob to be deleted while we
// still refer to it.
func (ba *spannerGCSBlobAccess) touchGCSObject(ctx context.Context, key string, t time.Time) error {
	gcsReftimeUpdateCount.Inc()
	obj := ba.gcsBucket.Object(key)
	attrs := storage.ObjectAttrsToUpdate{
		ContentType: "application/octet-stream",
		CustomTime:  t,
	}
	start := time.Now()
	_, err := obj.Update(ctx, attrs)
	backendOperationsDurationSeconds.WithLabelValues(ba.storageType, BE_GCS, BE_TOUCH).Observe(time.Now().Sub(start).Seconds())
	if err != nil {
		gcsReftimeUpdateFailedCount.Inc()
		log.Printf("Couldn't update CustomTime for %s: %v", key, err)
		return err
	}
	return nil
}

// Update the ReferenceTime field the Spanner blob.
func (ba *spannerGCSBlobAccess) touchSpannerObjects(ctx context.Context, keys []string, t time.Time) error {
	spannerReftimeUpdateCount.Inc()
	start := time.Now()
	_, err := ba.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Hopefully this is more efficient than doing a read-modify-write.  Tried InsertOrUpdateStruct(),
		// but that set the InlineData field to NULL, contradicting the manual page that "Any column values
		// not explicitly written are preserved."
		refTime := t.Format(time.RFC3339)
		stmt := spanner.NewStatement(`UPDATE ` + tableName + ` SET ReferenceTime = TIMESTAMP(@reftime) WHERE Key in unnest(@keys)`)
		stmt.Params["keys"] = keys
		stmt.Params["reftime"] = refTime
		_, err := txn.Update(ctx, stmt)
		if err != nil {
			spannerReftimeUpdateFailedCount.Inc()
			log.Printf("Can't update reftime in %d Blobs: %v", len(keys), err)
			return err
		}
		return nil
	})
	backendOperationsDurationSeconds.WithLabelValues(ba.storageType, BE_SPANNER, BE_TOUCH).Observe(time.Now().Sub(start).Seconds())
	return err
}

// Process deferred access time updates from action cache GET operations.
func (ba *spannerGCSBlobAccess) bulkUpdate(in <-chan keyLoc) {
	keys := make([]string, 0, maxRefBulkSz)
	locs := make([]int, 0, maxRefBulkSz)
	keyMap := make(map[string]bool, maxRefBulkSz) // used to dedup the list of keys
	t := time.NewTimer(maxRefHours * time.Hour)
	timedout := false
	for {
		select {
		case kl := <-in:
			if keyMap[kl.key] {
				log.Printf("SKIPPING duplicate key %s", kl.key)
			} else {
				keyMap[kl.key] = true
				keys = append(keys, kl.key)
				locs = append(locs, kl.loc)
			}
		case <-t.C:
			timedout = true
		}
		if (timedout && len(keys) != 0) || (len(keys) == maxRefBulkSz) {
			log.Printf("Processing %d delayed reftime updates", len(keys))
			timedout = true // just so we know to reset the timer if we're here because we've reached maxRefBulkSz
			now := time.Now().UTC()
			go ba.touchSpannerObjects(context.Background(), keys, now)

			// It's unlikely an AC entry would be so large, but I guess we should handle this just in case
			// it occurs.  I mean, looking at the ActionResult proto definition, it's possble for it to be
			// too large to fit in a spanner row.
			for idx, loc := range locs {
				if (loc & LOC_GCS) != 0 {
					go ba.touchGCSObject(context.Background(), keys[idx], now)
				}
			}
			keys = make([]string, 0, maxRefBulkSz)
			locs = make([]int, 0, maxRefBulkSz)
			keyMap = make(map[string]bool, maxRefBulkSz)
		}

		// We need to reset the timer if we timed out or if we processed a bulk transfer.  We could have timed
		// out without any work to do, so always check if timedout is true here so we can reset the timer.
		// Stay away from this pattern:
		//    if !t.Stop() {
		//        <-t.C
		//    }
		//    t.Reset(...)
		// It was racy and we'd sometimes block reading from the channel.
		if timedout {
			timedout = false
			t.Stop()
			t = time.NewTimer(maxRefHours * time.Hour)
		}
	}
}

type spannerGCSErrorHandler struct {
	start  time.Time
	sType  string
	beType string
	op     string
}

func (eh *spannerGCSErrorHandler) OnError(err error) (buffer.Buffer, error) {
	return nil, err
}

func (eh *spannerGCSErrorHandler) Done() {
	backendOperationsDurationSeconds.WithLabelValues(eh.sType, eh.beType, eh.op).Observe(time.Now().Sub(eh.start).Seconds())
}
