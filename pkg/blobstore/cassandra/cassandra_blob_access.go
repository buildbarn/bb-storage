package cassandra

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	bbdigest "github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"

	"golang.org/x/sync/errgroup"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	writeConsistency            = gocql.LocalQuorum
	readConsistency             = gocql.LocalQuorum
	lastAccessUpdateConsistency = gocql.LocalOne

	simultaneousWorkers = 100

	// The number of workers to use for updating the `last_access` field
	lastAccessUpdateWorkerCount = 50
)

var (
	cassandraBlobStorePrometheusMetrics sync.Once

	segmentWriteHist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "buildbarn",
		Subsystem: "storage",
		Name:      "cassandra_segment_write_duration_seconds",
		Help:      "Amount of time in seconds it takes to write each segment to Cassandra, measured in seconds",
		Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
	})

	segmentReadHist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "buildbarn",
		Subsystem: "storage",
		Name:      "cassandra_segment_read_duration_seconds",
		Help:      "Amount of time in seconds it takes to read each segment from Cassandra",
		Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
	})

	lastAccessUpdatesFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "buildbarn",
		Subsystem: "storage",
		Name:      "cassandra_last_access_failure_count",
		Help:      "Count of the number of times the `last_access` field could not be updated",
	})
)

type cassandraBlobAccess struct {
	capabilities.Provider
	readBufferFactory        blobstore.ReadBufferFactory
	segmentSize              int32
	session                  *gocql.Session
	lastAccessUpdateInterval time.Duration
	lastAccessUpdate         chan<- func()
	tables                   *tables
}

func NewCassandraSession(clusterHosts []string, keyspace string, preferredDc string, port, protoVersion int32, username, password string, tlsConfig *tls.Config) (*gocql.Session, error) {
	cluster := gocql.NewCluster(clusterHosts...)
	cluster.Keyspace = keyspace
	cluster.Consistency = writeConsistency

	if preferredDc != "" {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(preferredDc))
	}

	if port != 0 {
		cluster.Port = int(port)
	}

	if protoVersion != 0 {
		cluster.ProtoVersion = int(protoVersion)
	}

	if username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	if tlsConfig != nil {
		cluster.SslOpts = &gocql.SslOptions{
			Config: tlsConfig,
		}
	}

	return cluster.CreateSession()
}

// NewCassandraBlobAccess provides an implementation of storage backed by Cassandra.
func NewCassandraBlobAccess(capabilitiesProvider capabilities.Provider, readBufferFactory blobstore.ReadBufferFactory, session *gocql.Session, segmentSizeInBytes int32, lastAccessUpdateInterval time.Duration, tablePrefix string) blobstore.BlobAccess {
	cassandraBlobStorePrometheusMetrics.Do(func() {
		prometheus.MustRegister(segmentWriteHist)
		prometheus.MustRegister(segmentReadHist)
		prometheus.MustRegister(lastAccessUpdatesFailures)
	})

	if tablePrefix == "" {
		log.Printf("No table prefix set. Cowardly refusing to proceed.")
		return blobstore.NewErrorBlobAccess(errors.New("table prefix must be set"))
	}
	log.Printf("Table prefix: %s", tablePrefix)

	// Create the worker pool for updating the last access times
	jobs := make(chan func(), lastAccessUpdateWorkerCount)
	for i := 0; i < lastAccessUpdateWorkerCount; i++ {
		go lastUpdateWorker(jobs)
	}

	log.Printf("Cassandra storage ready.")

	return &cassandraBlobAccess{
		Provider:                 capabilitiesProvider,
		session:                  session,
		readBufferFactory:        readBufferFactory,
		segmentSize:              segmentSizeInBytes,
		lastAccessUpdateInterval: lastAccessUpdateInterval,
		lastAccessUpdate:         jobs,
		tables:                   newTables(tablePrefix, session),
	}
}

func logErrorIfNotCancelledContext(err error, msg string, args ...interface{}) {
	if err != nil && !errors.Is(err, context.Canceled) {
		initialMessage := fmt.Sprintf(msg, args...)
		log.Printf("%s: %v", initialMessage, err)
	}
}

func (a *cassandraBlobAccess) Get(ctx context.Context, digest bbdigest.Digest) buffer.Buffer {
	metadata, err := a.tables.metadata.read(ctx, digest)
	if err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return buffer.NewBufferFromError(status.Errorf(codes.NotFound, "No metadata found for digest %s", digest.String()))
		}
		return buffer.NewBufferFromError(err)
	}

	a.tables.metadata.updateLastAccessTime(context.Background(), digest, metadata.instanceName, metadata.lastAccess, a.lastAccessUpdateInterval, a.lastAccessUpdate)

	reader := sequentialReader{
		ctx:           ctx,
		session:       a.session,
		tableName:     a.tables.content.tableName,
		blobID:        metadata.blobID,
		segmentCount:  metadata.segmentCount,
		segmentNumber: 0,
		currentReader: nil,
		digest:        digest,
	}

	return a.readBufferFactory.NewBufferFromReader(digest, io.NopCloser(&reader), buffer.Irreparable(digest))
}

func (a *cassandraBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest bbdigest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	b, _ := slicer.Slice(a.Get(ctx, parentDigest), childDigest)
	return b
}

func (a *cassandraBlobAccess) Put(ctx context.Context, digest bbdigest.Digest, b buffer.Buffer) error {
	// We derive the blob id from the digest. The metadata table will control access to
	// this. It's okay for two streams to write the same data simultaneously because of
	// the way that Cassandra acts: because the primary key will be the same, the inserts
	// will effectively be updates, and since the data is identical, this will be safe.
	blobID := strings.Join([]string{
		digest.GetDigestFunction().GetEnumValue().String(),
		digest.GetHashString(),
		strconv.Itoa(int(digest.GetSizeBytes())),
		digest.GetInstanceName().String(),
	}, "-")

	estimatedSegmentCount := getSegmentCount(a.segmentSize, digest.GetSizeBytes())

	now := time.Now()
	var segmentCount int

	if a.tables.content.isSegmentPresent(ctx, blobID, estimatedSegmentCount) {
		segmentCount = estimatedSegmentCount
	} else {
		instanceName := digest.GetInstanceName().String()
		if err := a.tables.orphanedContent.insert(ctx, digest, blobID, instanceName, estimatedSegmentCount, now); err != nil {
			b.Discard()
			return err
		}
		defer a.tables.orphanedContent.delete(ctx, blobID, instanceName)

		actualCount, err := a.writeContentData(ctx, digest, b, blobID)
		if err != nil {
			return err
		}
		segmentCount = actualCount
	}

	return a.tables.metadata.update(ctx, digest, blobID, digest.GetInstanceName().String(), segmentCount, now, a.segmentSize)
}

func getSegmentCount(segmentSize int32, sizeInBytes int64) int {
	segmentSize64 := int64(segmentSize)

	return int((sizeInBytes + segmentSize64 - 1) / segmentSize64)
}

func (a *cassandraBlobAccess) writeContentData(ctx context.Context, digest bbdigest.Digest, b buffer.Buffer, blobID string) (int, error) {
	segmentCount := 0

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(simultaneousWorkers + 1)

	g.Go(func() error {
		reader := b.ToReader()
		defer reader.Close()

		for {
			// Read either up to EOF or until we have `segmentSize` bytes
			buf := new(bytes.Buffer)
			_, err := io.CopyN(buf, reader, int64(a.segmentSize))
			isEOF := err == io.EOF

			if err != nil && err != io.EOF {
				logErrorIfNotCancelledContext(err, "Put: Failed to read next segment to write from client. %d of %s (digest function: %s, size: %d)", segmentCount, digest.GetHashString(), digest.GetDigestFunction().GetEnumValue().String(), digest.GetSizeBytes())
				return err
			}

			count := segmentCount
			g.Go(func() error {
				return a.tables.content.insertSegment(gCtx, blobID, buf.Bytes(), count, digest.GetHashString())
			})

			segmentCount++

			if isEOF {
				break
			}
		}

		// TODO: Ensure hashes match at this point
		// TODO: Delete from the blobs table if hashes do not match

		return nil
	})

	err := g.Wait()
	if err != nil {
		logErrorIfNotCancelledContext(err, "Put: Unable to store %s", digest.String())
		return 0, err
	}
	return segmentCount, nil
}

func (a *cassandraBlobAccess) FindMissing(ctx context.Context, digests bbdigest.Set) (bbdigest.Set, error) {
	// The obvious thing to do here is to create a `SELECT ... IN (?, ?)` for each digest, but doing so
	// prevents token-aware routing of queries. Instead, we issue a single `SELECT` per digest and
	// aggregate the values that come back.

	builder := bbdigest.NewSetBuilder()
	var mu sync.Mutex

	cancellableCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, errCtx := errgroup.WithContext(cancellableCtx)

	sem := make(chan struct{}, simultaneousWorkers)

	// Try not to be a terrible, terrible citizen
	for _, digest := range digests.Items() {
		sem <- struct{}{}

		g.Go(func() error {
			defer func() {
				<-sem
			}()

			err := a.isMissing(errCtx, digest, &builder, &mu)
			if err != nil {
				cancel()
				return err
			}
			return nil
		})
	}
	err := g.Wait()
	if err != nil {
		logErrorIfNotCancelledContext(err, "FindMissing: returning early because of error reading data")
		return bbdigest.EmptySet, err
	}

	toReturn := builder.Build()

	return toReturn, nil
}

func (a *cassandraBlobAccess) isMissing(ctx context.Context, digest bbdigest.Digest, builder *bbdigest.SetBuilder, mu *sync.Mutex) error {
	metadata, err := a.tables.metadata.read(ctx, digest)
	if err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			mu.Lock()
			defer mu.Unlock()
			builder.Add(digest)
			return nil
		}

		return err
	}

	a.tables.metadata.updateLastAccessTime(context.Background(), digest, metadata.instanceName, metadata.lastAccess, a.lastAccessUpdateInterval, a.lastAccessUpdate)

	return nil
}

func lastUpdateWorker(jobs chan func()) {
	for j := range jobs {
		j()
	}
}

type tables struct {
	content         *contentTable
	metadata        *metadataTable
	orphanedContent *orphanedContentTable
}

func newTables(prefix string, session *gocql.Session) *tables {
	return &tables{
		content: &contentTable{
			tableName: prefix + "_content",
			session:   session,
		},
		metadata: &metadataTable{
			tableName: prefix + "_metadata",
			session:   session,
		},
		orphanedContent: &orphanedContentTable{
			tableName: prefix + "_orphaned_content",
			session:   session,
		},
	}
}

type contentTable struct {
	tableName string
	session   *gocql.Session
}

func (t *contentTable) insertSegment(ctx context.Context, blobID string, data []byte, segment int, digestHash string) error {
	defer func(start time.Time) {
		segmentWriteHist.Observe(float64(time.Since(start).Seconds()))
	}(time.Now())

	if err := retryCassandraWrite(
		ctx,
		t.session,
		writeConsistency,
		fmt.Sprintf("INSERT INTO %s (blob_id, segment, content) VALUES(?, ?, ?)", t.tableName),
		blobID,
		segment,
		data,
	); err != nil {
		logErrorIfNotCancelledContext(err, "Put: unable to write to cassandra for blobID %s (%s) and segment %d", blobID, digestHash, segment)
		return err
	}

	return nil
}

func (t *contentTable) isSegmentPresent(ctx context.Context, blobID string, segment int) bool {
	// Quick check to see if the data is already present in the `contents` table. This
	// might be the case if we wrote with one instance name, but now use another. We
	// look for the last segment.
	err := t.session.Query(
		fmt.Sprintf("SELECT blob_id FROM %s WHERE blob_id = ? AND segment = ?", t.tableName),
		blobID,
		segment,
	).WithContext(ctx).Consistency(readConsistency).Idempotent(true).Exec()
	return errors.Is(err, gocql.ErrNotFound)
}

type metadataTable struct {
	tableName string
	session   *gocql.Session
}

// read queries the metadata table to check if a blob exists.
// It returns:
// - gocql.ErrNotFound when no row, or a row with empty blobID is found
// - a wrapped gocql error when the Cassandra query failed
func (t *metadataTable) read(ctx context.Context, digest bbdigest.Digest) (*metadataTableRow, error) {
	var row metadataTableRow
	var err error

	// We check the universal pool first
	for _, name := range []string{"", digest.GetInstanceName().String()} {
		err = t.session.Query(
			fmt.Sprintf("SELECT digest_instance_name, last_access, segment_size, segment_count, blob_id FROM %s WHERE digest_function = ? AND digest_hash = ? AND digest_size_bytes = ? AND digest_instance_name = ? LIMIT 1", t.tableName),
			digest.GetDigestFunction().GetEnumValue().String(),
			digest.GetHashString(),
			digest.GetSizeBytes(),
			name,
		).WithContext(ctx).Consistency(readConsistency).Idempotent(true).Scan(&row.instanceName, &row.lastAccess, &row.segmentSize, &row.segmentCount, &row.blobID)

		if err == nil && row.blobID != "" {
			break
		}
	}

	if err != nil && !errors.Is(err, gocql.ErrNotFound) {
		logErrorIfNotCancelledContext(err, "metadataTable.read: Cassandra error reading metadata for digest %s", digest.String())
		return nil, fmt.Errorf("cassandra error reading metadata for digest %s: %w", digest.String(), err)
	}

	if errors.Is(err, gocql.ErrNotFound) || row.blobID == "" {
		return nil, gocql.ErrNotFound
	}

	return &row, nil
}

func (t *metadataTable) update(ctx context.Context, digest bbdigest.Digest, blobID, digestInstanceName string, segmentCount int, now time.Time, segmentSize int32) error {
	// Finally update metadata table. Note that we use a broader `writeConsistency` here, so that
	// if we fail over the metadata is correct in all datacenters. This may require a cross-DC read
	// later on, but in most cases this won't be necessary as we don't expect to be constantly
	// flipping between DCs. We only do this on a `put` (and not when updating `last_access`)
	// because the `last_access` time is far less important than whether the data is present.
	if err := retryCassandraWrite(
		ctx,
		t.session,
		gocql.EachQuorum,
		fmt.Sprintf("INSERT INTO %s (digest_function, digest_hash, digest_size_bytes, digest_instance_name, blob_id, segment_count, segment_size, last_access) VALUES(?, ?, ?, ?, ?, ?, ?, ?)", t.tableName),
		digest.GetDigestFunction().GetEnumValue().String(),
		digest.GetHashString(),
		digest.GetSizeBytes(),
		digestInstanceName,
		blobID,
		segmentCount,
		segmentSize,
		now,
	); err != nil {
		logErrorIfNotCancelledContext(err, "Put: Unable to update info table for hash %s", digest.String())
		return err
	}

	return nil
}

func (t *metadataTable) updateLastAccessTime(ctx context.Context, digest bbdigest.Digest, instanceName string, lastAccessed time.Time, lastAccessUpdateInterval time.Duration, lastAccessUpdate chan<- func()) {
	// Note, we are "adding" a negative duration. `Sub` takes a `time.Time` and returns a `time.Duration`, which
	// is the opposite of what we want.
	window := time.Now().Add(-lastAccessUpdateInterval)
	if lastAccessed.After(window) {
		return
	}

	updaterFunc := func() {
		// This query might insert a row with (blob_id, segment_count, segment_size) == (null, null, null), which is fine.
		// Such a row should be considered missing from the POV of the service. Eventually, it will either be updated again
		// with a blobID or (more likely) be reaped.
		// We do not use `IF EXISTS` because it would make the query `SERIAL`, meaning it needs to be coordinated between
		// all the replicas, which is very expensive (think of it as having to take a distributed lock for that row).
		if err := retryCassandraWrite(
			ctx,
			t.session,
			lastAccessUpdateConsistency,
			fmt.Sprintf("UPDATE %s SET last_access = ? WHERE digest_function = ? AND digest_hash = ? AND digest_size_bytes = ? AND digest_instance_name = ?", t.tableName),
			time.Now(),
			digest.GetDigestFunction().GetEnumValue().String(),
			digest.GetHashString(),
			digest.GetSizeBytes(),
			instanceName,
		); err != nil {
			// There's nothing sensible we can do to recover here, but at least log the error
			logErrorIfNotCancelledContext(err, "Unable to update last access time for %s", digest.String())
			lastAccessUpdatesFailures.Inc()
		}
	}

	select {
	case lastAccessUpdate <- updaterFunc:
		// Fantastic. We're able to update the last access time.
	default:
		// This isn't ideal, but one of two things is going to happen:
		//  1. The digest won't be accessed again, and will age out sooner
		//  2. The digest will be accessed again, and maybe next time we'll
		//     be able to update it.
		// Either way, we're making the trade-off of a possible inability to
		// update _everything_ properly for not blocking other operations in
		// this blobstore.
	}
}

type metadataTableRow struct {
	instanceName string
	blobID       string
	segmentSize  int
	segmentCount int
	lastAccess   time.Time
}

type orphanedContentTable struct {
	tableName string
	session   *gocql.Session
}

func (t *orphanedContentTable) delete(ctx context.Context, blobID, digestInstanceName string) {
	_ = retryCassandraWrite(
		ctx,
		t.session,
		writeConsistency,
		fmt.Sprintf("DELETE FROM %s WHERE blob_id = ? AND digest_instance_name = ?", t.tableName),
		blobID,
		digestInstanceName,
	)
}

func (t *orphanedContentTable) insert(ctx context.Context, digest bbdigest.Digest, blobID, digestInstanceName string, estimatedSegmentCount int, now time.Time) error {
	if err := retryCassandraWrite(
		ctx,
		t.session,
		writeConsistency,
		fmt.Sprintf("INSERT INTO %s (digest_function, digest_hash, digest_size_bytes, digest_instance_name, blob_id, segment_count, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)", t.tableName),
		digest.GetDigestFunction().GetEnumValue().String(),
		digest.GetHashString(),
		digest.GetSizeBytes(),
		digestInstanceName,
		blobID,
		estimatedSegmentCount,
		now,
	); err != nil {
		logErrorIfNotCancelledContext(err, "Unable to insert into orphan table for %s", digest.String())
		return err
	}

	return nil
}

type sequentialReader struct {
	ctx           context.Context
	session       *gocql.Session
	tableName     string
	segmentCount  int
	blobID        string
	segmentNumber int
	currentReader io.Reader
	digest        bbdigest.Digest
}

func (s *sequentialReader) Read(p []byte) (n int, err error) {
	if s.currentReader == nil {
		err := s.fetchNext()
		if err != nil {
			logErrorIfNotCancelledContext(err, "Read for %s (blob id %s) failed", s.digest.String(), s.blobID)
			return 0, err
		}
	}

	read, err := s.currentReader.Read(p)
	if err != nil {
		if err != io.EOF {
			// If something went wrong, and it wasn't the end of the reader, bail
			logErrorIfNotCancelledContext(err, "Get: Unable to fully read blob %s of %s", s.blobID, s.digest.String())
			return read, err
		}

		// If we've read the end of the final segment, we're done!
		if s.segmentNumber == s.segmentCount {
			// Making it explicit that `err` is already `io.EOF`
			return read, io.EOF
		}

		// Otherwise, prep for the next read
		err := s.fetchNext()
		if err != nil {
			return read, err
		}
	}

	return read, nil
}

func (s *sequentialReader) fetchNext() error {
	defer func(start time.Time) {
		segmentReadHist.Observe(float64(time.Since(start).Seconds()))
	}(time.Now())

	var data []byte
	err := s.session.Query(
		fmt.Sprintf("SELECT content FROM %s WHERE blob_id = ? AND segment = ? LIMIT 1", s.tableName),
		s.blobID,
		s.segmentNumber,
	).WithContext(s.ctx).Consistency(readConsistency).Idempotent(true).Scan(&data)
	s.segmentNumber++
	if err != nil {
		logErrorIfNotCancelledContext(err, "Unable to read segment %d of %s", s.segmentNumber, s.digest.String())
		return err
	}
	s.currentReader = bytes.NewReader(data)
	return nil
}

func retryCassandraWrite(ctx context.Context, session *gocql.Session, consistency gocql.Consistency, query string, values ...interface{}) error {
	// Attempt to write the segment up to three times.
	var err error
	maxTries := 3
	cqlQuery := session.Query(query, values...).WithContext(ctx).Consistency(consistency).Idempotent(true)
	for i := 0; i < maxTries; i++ {
		if i > 0 {
			// Sleep for ~500ms, then ~5s.
			duration := time.Duration(rand.Intn(int(math.Pow10(i + 2))))
			err := sleep(ctx, duration*time.Millisecond)
			if errors.Is(err, context.Canceled) {
				return err
			}
		}

		err = cqlQuery.Exec()
		if err == nil {
			return nil
		}

		// If the context is cancelled, then there's not much else we can do.
		if errors.Is(err, context.Canceled) {
			return err
		}

		logErrorIfNotCancelledContext(err, "Failed to write to backend (try %d/%d): %s", i+1, maxTries, formatQuery(cqlQuery))
	}

	return err
}

func sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
