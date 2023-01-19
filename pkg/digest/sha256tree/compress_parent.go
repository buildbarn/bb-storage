package sha256tree

import (
	"math/bits"
)

// The first 32 bits of the fractional parts of the cube roots of the
// first 64 prime numbers.
var k = []uint32{
	0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
	0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
	0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
	0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
	0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
	0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
	0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
	0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
	0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
	0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
	0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
	0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
	0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
	0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
	0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
	0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
}

// Given hashes of left and right children, compute the hash of the
// parent. This is the default implementation that is used in case no
// hardware acceleration is available.
var compressParent = func(left, right, output *[Size / 4]uint32) {
	// Let the message be the concatenation of the hashes of the
	// left and right children.
	var w [64]uint32
	for i := 0; i < 8; i++ {
		w[i] = left[i]
	}
	for i := 0; i < 8; i++ {
		w[i+8] = right[i]
	}
	for i := 16; i < 64; i++ {
		wi2 := w[i-2]
		sigma1 := bits.RotateLeft32(wi2, -17) ^ bits.RotateLeft32(wi2, -19) ^ (wi2 >> 10)
		wi15 := w[i-15]
		sigma0 := bits.RotateLeft32(wi15, -7) ^ bits.RotateLeft32(wi15, -18) ^ (wi15 >> 3)
		w[i] = sigma1 + w[i-7] + sigma0 + w[i-16]
	}

	// Perform the rounds. Let the initial hash state be the first
	// 32 bits of the fractional parts of the square roots of primes
	// 23 to 53, which is different from the ones used to hash data.
	a := uint32(0xcbbb9d5d)
	b := uint32(0x629a292a)
	c := uint32(0x9159015a)
	d := uint32(0x152fecd8)
	e := uint32(0x67332667)
	f := uint32(0x8eb44a87)
	g := uint32(0xdb0c2e0d)
	h := uint32(0x47b5481d)
	for i := 0; i < 64; i++ {
		sigma1 := bits.RotateLeft32(e, -6) ^ bits.RotateLeft32(e, -11) ^ bits.RotateLeft32(e, -25)
		ch := (e & f) ^ (^e & g)
		t1 := h + sigma1 + ch + k[i] + w[i]

		sigma0 := bits.RotateLeft32(a, -2) ^ bits.RotateLeft32(a, -13) ^ bits.RotateLeft32(a, -22)
		maj := (a & b) ^ (a & c) ^ (b & c)
		t2 := sigma0 + maj

		h = g
		g = f
		f = e
		e = d + t1
		d = c
		c = b
		b = a
		a = t1 + t2
	}

	*output = [Size / 4]uint32{a, b, c, d, e, f, g, h}
}
