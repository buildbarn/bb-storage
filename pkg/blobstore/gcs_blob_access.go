package blobstore

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"

	"cloud.google.com/go/storage"
	"github.com/buildbarn/bb-storage/pkg/util"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type gcs struct {
	c             *storage.Client
	bucket        string
	keyPrefix     string
	blobKeyFormat util.DigestKeyFormat
}

// NewGCS creates a new GCS BlobAccess that uses the provide GCS bucket as its
// backing store.
func NewGCS(ctx context.Context, bucketName, keyPrefix string, blobKeyFormat util.DigestKeyFormat, authFile string) (BlobAccess, error) {
	var creds *google.Credentials
	if authFile == "" {
		var err error
		creds, err = google.FindDefaultCredentials(ctx, storage.ScopeReadWrite)
		if err != nil {
			return nil, err
		}
	} else {
		data, err := ioutil.ReadFile(authFile)
		if err != nil {
			return nil, err
		}
		creds, err = google.CredentialsFromJSON(ctx, data, storage.ScopeReadWrite)
		if err != nil {
			return nil, err
		}
	}
	client, err := storage.NewClient(ctx, option.WithCredentials(creds))
	if err != nil {
		return nil, err
	}
	return &gcs{
		c:             client,
		keyPrefix:     keyPrefix,
		bucket:        bucketName,
		blobKeyFormat: blobKeyFormat,
	}, nil
}

// Get fetches the object referenced by digest.
func (g *gcs) Get(ctx context.Context, digest *util.Digest) (int64, io.ReadCloser, error) {
	o := g.get(digest)
	a, err := o.Attrs(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to get %q: %v", digest, err)
	}
	r, err := o.NewReader(ctx)
	if err != nil {
		return 0, nil, err
	}
	return a.Size, r, nil
}

// Put uploads the object referenced by digest.
func (g *gcs) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
	defer r.Close()
	w := g.get(digest).NewWriter(ctx)
	if _, err := io.Copy(w, r); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return nil
}

// Delete deletes the object referenced by digest.
func (g *gcs) Delete(ctx context.Context, digest *util.Digest) error {
	return g.get(digest).Delete(ctx)
}

// FindMissing validates the existence of digests.
func (g *gcs) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	var missing []*util.Digest
	for _, digest := range digests {
		_, err := g.get(digest).Attrs(ctx)
		switch {
		case err == storage.ErrObjectNotExist:
			missing = append(missing, digest)
		case err != nil:
			return nil, err
		}
	}
	return missing, nil
}

func (g *gcs) getKey(digest *util.Digest) string {
	return g.keyPrefix + digest.GetKey(g.blobKeyFormat)
}

func (g *gcs) get(digest *util.Digest) *storage.ObjectHandle {
	return g.c.Bucket(g.bucket).Object(g.getKey(digest))
}
