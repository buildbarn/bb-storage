package blobstore

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"log"
	"time"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/dgraph-io/badger"
	duration "github.com/golang/protobuf/ptypes/duration"
	"go.opencensus.io/trace"
	"google.golang.org/grpc/codes"
)

const (
	defaultTTL = 10 * 24 * time.Hour
)

type badgerBlobAccess struct {
	db            *badger.DB
	blobKeyFormat util.DigestKeyFormat
	ttl           time.Duration
}

// NewBadgerBlobAccess creates a BlobAccess that uses Badger as its backing store.
func NewBadgerBlobAccess(db *badger.DB, blobKeyFormat util.DigestKeyFormat, ttl *duration.Duration) BlobAccess {
	ba := &badgerBlobAccess{
		db:            db,
		blobKeyFormat: blobKeyFormat,
		ttl:           defaultTTL,
	}
	if ttl != nil {
		ba.ttl = time.Duration(ttl.Seconds) * time.Second
	}
	go ba.gc()
	return ba
}

// Badger storage garbage collection
func (ba *badgerBlobAccess) gc() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
	again:
		err := ba.db.RunValueLogGC(0.7)
		if err == nil {
			goto again
		}
	}
}

func (ba *badgerBlobAccess) Get(ctx context.Context, digest *util.Digest) (int64, io.ReadCloser, error) {
	ctx, span := trace.StartSpan(ctx, "badgerBlobAccess.Get")
	defer span.End()
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	var value []byte
	err := ba.db.View(func(txn *badger.Txn) error {
		span.Annotate(nil, "Transaction created")
		item, err := txn.Get([]byte(digest.GetKey(ba.blobKeyFormat)))
		span.Annotate(nil, "Get finished")
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		span.Annotate(nil, "ValueCopy finished")
		return err
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return 0, nil, util.StatusWrapWithCode(err, codes.NotFound, "Failed to get blob")
		}
		return 0, nil, util.StatusWrapWithCode(err, codes.Unavailable, "Failed to get blob")
	}
	return int64(len(value)), ioutil.NopCloser(bytes.NewBuffer(value)), nil
}

func (ba *badgerBlobAccess) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
	ctx, span := trace.StartSpan(ctx, "badgerBlobAccess.Put")
	defer span.End()
	if err := ctx.Err(); err != nil {
		r.Close()
		return err
	}
	value, err := ioutil.ReadAll(r)
	r.Close()
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Unavailable, "Failed to put blob")
	}
	span.Annotate(nil, "creating Transaction")
	txn := ba.db.NewTransaction(true)
	defer txn.Discard()
	span.Annotate(nil, "Transaction created")
	err = txn.SetWithTTL([]byte(digest.GetKey(ba.blobKeyFormat)), value, ba.ttl)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Unavailable, "Failed to set blob")
	}
	span.Annotate(nil, "Set finished")
	// Provide a callback so write is asynchronous.
	err = txn.Commit(func(err error) {
		if err != nil {
			log.Println("Commit failed:", err)
		}
	})
	span.Annotate(nil, "Commit finished")
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Unavailable, "Failed to commit transaction")
	}
	return nil
}

func (ba *badgerBlobAccess) Delete(ctx context.Context, digest *util.Digest) error {
	ctx, span := trace.StartSpan(ctx, "badgerBlobAccess.Put")
	defer span.End()
	txn := ba.db.NewTransaction(true)
	defer txn.Discard()
	span.Annotate(nil, "Transaction created")
	err := txn.Delete([]byte(digest.GetKey(ba.blobKeyFormat)))
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Unavailable, "Failed to delete blob")
	}
	span.Annotate(nil, "Delete finished")
	// Provide a callback so write is asynchronous.
	err = txn.Commit(func(err error) {
		if err != nil {
			log.Println("Commit failed:", err)
		}
	})
	if err != nil {
		return util.StatusWrapWithCode(err, codes.Unavailable, "Failed to commit transaction")
	}
	span.Annotate(nil, "Commit finished")
	return nil
}

func (ba *badgerBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(digests) == 0 {
		return nil, nil
	}
	txn := ba.db.NewTransaction(false)
	defer txn.Discard()
	var missing []*util.Digest
	it := txn.NewIterator(badger.IteratorOptions{PrefetchValues: false})
	defer it.Close()
	for _, digest := range digests {
		key := []byte(digest.GetKey(ba.blobKeyFormat))
		it.Seek(key)
		if !it.ValidForPrefix(key) {
			missing = append(missing, digest)
		}
	}
	return missing, nil
}
