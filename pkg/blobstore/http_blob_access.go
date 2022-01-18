package blobstore

import (
	"context"
	"fmt"
	"net/http"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type httpBlobAccess struct {
	address           string
	prefix            string
	readBufferFactory ReadBufferFactory
	httpClient        *http.Client
}

func convertHTTPUnexpectedStatus(resp *http.Response) error {
	return status.Errorf(codes.Unknown, "Unexpected status code from remote cache: %d - %s", resp.StatusCode, http.StatusText(resp.StatusCode))
}

// NewHTTPBlobAccess for use of HTTP/1.1 cache backend.
//
// See: https://docs.bazel.build/versions/master/remote-caching.html#http-caching-protocol
func NewHTTPBlobAccess(address, prefix string, readBufferFactory ReadBufferFactory, httpClient *http.Client) BlobAccess {
	return &httpBlobAccess{
		address:           address,
		prefix:            prefix,
		readBufferFactory: readBufferFactory,
		httpClient:        httpClient,
	}
}

func (ba *httpBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	url := fmt.Sprintf("%s/%s/%s", ba.address, ba.prefix, digest.GetHashString())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return buffer.NewBufferFromError(err)
	}
	resp, err := ba.httpClient.Do(req)
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

func (ba *httpBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	sizeBytes, err := b.GetSizeBytes()
	if err != nil {
		b.Discard()
		return err
	}
	url := fmt.Sprintf("%s/%s/%s", ba.address, ba.prefix, digest.GetHashString())
	r := b.ToReader()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, r)
	if err != nil {
		r.Close()
		return err
	}
	req.ContentLength = sizeBytes
	resp, err := ba.httpClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return convertHTTPUnexpectedStatus(resp)
	}
	return nil
}

func (ba *httpBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	missing := digest.NewSetBuilder()
	for _, blobDigest := range digests.Items() {
		url := fmt.Sprintf("%s/%s/%s", ba.address, ba.prefix, blobDigest.GetHashString())
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
		if err != nil {
			return digest.EmptySet, util.StatusWrapWithCode(err, codes.Internal, "Failed to create HTTP request")
		}
		resp, err := ba.httpClient.Do(req)
		if err != nil {
			return digest.EmptySet, err
		}
		resp.Body.Close()

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
