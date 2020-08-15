package blobstore

import (
	"context"
	"fmt"
	"net/http"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"

	// TODO: Migrate this code away from ctxhttp. Use the HTTPClient
	// interface that's in this package instead. This allows us to
	// add unit testing coverage.
	"golang.org/x/net/context/ctxhttp"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type remoteBlobAccess struct {
	address           string
	prefix            string
	readBufferFactory ReadBufferFactory
}

func convertHTTPUnexpectedStatus(resp *http.Response) error {
	return status.Errorf(codes.Unknown, "Unexpected status code from remote cache: %d - %s", resp.StatusCode, http.StatusText(resp.StatusCode))
}

// NewRemoteBlobAccess for use of HTTP/1.1 cache backend.
//
// See: https://docs.bazel.build/versions/master/remote-caching.html#http-caching-protocol
func NewRemoteBlobAccess(address string, prefix string, readBufferFactory ReadBufferFactory) BlobAccess {
	return &remoteBlobAccess{
		address:           address,
		prefix:            prefix,
		readBufferFactory: readBufferFactory,
	}
}

func (ba *remoteBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	url := fmt.Sprintf("%s/%s/%s", ba.address, ba.prefix, digest.GetHashString())
	resp, err := ctxhttp.Get(ctx, http.DefaultClient, url)
	if err != nil {
		return buffer.NewBufferFromError(err)
	}

	switch resp.StatusCode {
	case http.StatusNotFound:
		resp.Body.Close()
		return buffer.NewBufferFromError(status.Error(codes.NotFound, url))
	case http.StatusOK:
		return ba.readBufferFactory.NewBufferFromReader(digest, resp.Body, buffer.Irreparable(digest))
	default:
		resp.Body.Close()
		return buffer.NewBufferFromError(convertHTTPUnexpectedStatus(resp))
	}
}

func (ba *remoteBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	sizeBytes, err := b.GetSizeBytes()
	if err != nil {
		b.Discard()
		return err
	}
	url := fmt.Sprintf("%s/%s/%s", ba.address, ba.prefix, digest.GetHashString())
	r := b.ToReader()
	req, err := http.NewRequest(http.MethodPut, url, r)
	if err != nil {
		r.Close()
		return err
	}
	req.ContentLength = sizeBytes
	resp, err := ctxhttp.Do(ctx, http.DefaultClient, req)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return convertHTTPUnexpectedStatus(resp)
	}
	return nil
}

func (ba *remoteBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	missing := digest.NewSetBuilder()
	for _, blobDigest := range digests.Items() {
		url := fmt.Sprintf("%s/%s/%s", ba.address, ba.prefix, blobDigest.GetHashString())
		resp, err := ctxhttp.Head(ctx, http.DefaultClient, url)
		if err != nil {
			return digest.EmptySet, err
		}

		switch resp.StatusCode {
		case http.StatusNotFound:
			missing.Add(blobDigest)
		case http.StatusOK:
			continue
		default:
			return digest.EmptySet, convertHTTPUnexpectedStatus(resp)
		}
	}

	return missing.Build(), nil
}
