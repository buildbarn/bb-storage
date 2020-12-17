package digest

import (
	"encoding/hex"
	"hash"
)

// Function for computing new Digest objects. Function is a tuple of the
// REv2 instance name and hashing algorithm.
type Function struct {
	instanceName  InstanceName
	hasherFactory func() hash.Hash
}

// GetInstanceName returns the instance name that Digest objects would
// use if they were created from this Function.
func (f Function) GetInstanceName() InstanceName {
	return f.instanceName
}

// NewGenerator creates a writer that may be used to compute digests of
// newly created files.
func (f Function) NewGenerator() *Generator {
	return &Generator{
		instanceName: f.instanceName,
		partialHash:  f.hasherFactory(),
	}
}

// Generator is a writer that may be used to compute digests of newly
// created files.
type Generator struct {
	instanceName InstanceName
	partialHash  hash.Hash
	sizeBytes    int64
}

// Write a chunk of data from a newly created file into the state of the
// Generator.
func (dg *Generator) Write(p []byte) (int, error) {
	n, err := dg.partialHash.Write(p)
	dg.sizeBytes += int64(n)
	return n, err
}

// Sum creates a new digest based on the data written into the
// Generator.
func (dg *Generator) Sum() Digest {
	return dg.instanceName.newDigestUnchecked(
		hex.EncodeToString(dg.partialHash.Sum(nil)),
		dg.sizeBytes)
}
