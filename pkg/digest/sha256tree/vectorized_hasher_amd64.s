//go:build amd64

#include "textflag.h"

// Shuffle control mask for VPSHUFB to parse big endian input.
DATA endian<>+0(SB)/4, $0x00010203
DATA endian<>+4(SB)/4, $0x04050607
DATA endian<>+8(SB)/4, $0x08090a0b
DATA endian<>+12(SB)/4, $0x0c0d0e0f
DATA endian<>+16(SB)/4, $0x10111213
DATA endian<>+20(SB)/4, $0x14151617
DATA endian<>+24(SB)/4, $0x18191a1b
DATA endian<>+28(SB)/4, $0x1c1d1e1f
GLOBL endian<>(SB), RODATA|NOPTR, $32

// First 32 bits of the fractional parts of the square roots of primes
// 2 to 19.
DATA iv_data<>+0(SB)/4, $0x6a09e667
DATA iv_data<>+4(SB)/4, $0xbb67ae85
DATA iv_data<>+8(SB)/4, $0x3c6ef372
DATA iv_data<>+12(SB)/4, $0xa54ff53a
DATA iv_data<>+16(SB)/4, $0x510e527f
DATA iv_data<>+20(SB)/4, $0x9b05688c
DATA iv_data<>+24(SB)/4, $0x1f83d9ab
DATA iv_data<>+28(SB)/4, $0x5be0cd19
GLOBL iv_data<>(SB), RODATA|NOPTR, $32

// First 32 bits of the fractional parts of the square roots of primes
// 23 to 53.
DATA iv_parent<>+0(SB)/4, $0xcbbb9d5d
DATA iv_parent<>+4(SB)/4, $0x629a292a
DATA iv_parent<>+8(SB)/4, $0x9159015a
DATA iv_parent<>+12(SB)/4, $0x152fecd8
DATA iv_parent<>+16(SB)/4, $0x67332667
DATA iv_parent<>+20(SB)/4, $0x8eb44a87
DATA iv_parent<>+24(SB)/4, $0xdb0c2e0d
DATA iv_parent<>+28(SB)/4, $0x47b5481d
GLOBL iv_parent<>(SB), RODATA|NOPTR, $32

// Merkle-Damgard message padding.
DATA padding<>+0(SB)/4, $0x80000000
DATA padding<>+4(SB)/4, $0x00000000
DATA padding<>+8(SB)/4, $0x00002000
GLOBL padding<>(SB), RODATA|NOPTR, $12

// TODO: Any way we can share with the K table in compress_parent.go?
DATA k<>+0(SB)/4, $0x428a2f98
DATA k<>+4(SB)/4, $0x71374491
DATA k<>+8(SB)/4, $0xb5c0fbcf
DATA k<>+12(SB)/4, $0xe9b5dba5
DATA k<>+16(SB)/4, $0x3956c25b
DATA k<>+20(SB)/4, $0x59f111f1
DATA k<>+24(SB)/4, $0x923f82a4
DATA k<>+28(SB)/4, $0xab1c5ed5
DATA k<>+32(SB)/4, $0xd807aa98
DATA k<>+36(SB)/4, $0x12835b01
DATA k<>+40(SB)/4, $0x243185be
DATA k<>+44(SB)/4, $0x550c7dc3
DATA k<>+48(SB)/4, $0x72be5d74
DATA k<>+52(SB)/4, $0x80deb1fe
DATA k<>+56(SB)/4, $0x9bdc06a7
DATA k<>+60(SB)/4, $0xc19bf174
DATA k<>+64(SB)/4, $0xe49b69c1
DATA k<>+68(SB)/4, $0xefbe4786
DATA k<>+72(SB)/4, $0x0fc19dc6
DATA k<>+76(SB)/4, $0x240ca1cc
DATA k<>+80(SB)/4, $0x2de92c6f
DATA k<>+84(SB)/4, $0x4a7484aa
DATA k<>+88(SB)/4, $0x5cb0a9dc
DATA k<>+92(SB)/4, $0x76f988da
DATA k<>+96(SB)/4, $0x983e5152
DATA k<>+100(SB)/4, $0xa831c66d
DATA k<>+104(SB)/4, $0xb00327c8
DATA k<>+108(SB)/4, $0xbf597fc7
DATA k<>+112(SB)/4, $0xc6e00bf3
DATA k<>+116(SB)/4, $0xd5a79147
DATA k<>+120(SB)/4, $0x06ca6351
DATA k<>+124(SB)/4, $0x14292967
DATA k<>+128(SB)/4, $0x27b70a85
DATA k<>+132(SB)/4, $0x2e1b2138
DATA k<>+136(SB)/4, $0x4d2c6dfc
DATA k<>+140(SB)/4, $0x53380d13
DATA k<>+144(SB)/4, $0x650a7354
DATA k<>+148(SB)/4, $0x766a0abb
DATA k<>+152(SB)/4, $0x81c2c92e
DATA k<>+156(SB)/4, $0x92722c85
DATA k<>+160(SB)/4, $0xa2bfe8a1
DATA k<>+164(SB)/4, $0xa81a664b
DATA k<>+168(SB)/4, $0xc24b8b70
DATA k<>+172(SB)/4, $0xc76c51a3
DATA k<>+176(SB)/4, $0xd192e819
DATA k<>+180(SB)/4, $0xd6990624
DATA k<>+184(SB)/4, $0xf40e3585
DATA k<>+188(SB)/4, $0x106aa070
DATA k<>+192(SB)/4, $0x19a4c116
DATA k<>+196(SB)/4, $0x1e376c08
DATA k<>+200(SB)/4, $0x2748774c
DATA k<>+204(SB)/4, $0x34b0bcb5
DATA k<>+208(SB)/4, $0x391c0cb3
DATA k<>+212(SB)/4, $0x4ed8aa4a
DATA k<>+216(SB)/4, $0x5b9cca4f
DATA k<>+220(SB)/4, $0x682e6ff3
DATA k<>+224(SB)/4, $0x748f82ee
DATA k<>+228(SB)/4, $0x78a5636f
DATA k<>+232(SB)/4, $0x84c87814
DATA k<>+236(SB)/4, $0x8cc70208
DATA k<>+240(SB)/4, $0x90befffa
DATA k<>+244(SB)/4, $0xa4506ceb
DATA k<>+248(SB)/4, $0xbef9a3f7
DATA k<>+252(SB)/4, $0xc67178f2
GLOBL k<>(SB), RODATA|NOPTR, $256

// Align the stack to 32 bytes, so that we can efficiently load and
// store 256-bit vectors.
#define ALIGN_STACK		\
	LEAQ 0x1f(SP), DI	\
	MOVQ $0x1f, DX		\
	NOTQ DX			\
	ANDQ DX, DI

// Implementation of the lowercase sigma function.
#define LSIGMA(in, shift0, shift1, shift2, out, tmp) \
	VPSRLD	$shift0, in, out			\
	VPSLLD	$(32-shift0), in, tmp			\
	VPXOR	out, tmp, out				\
	VPSRLD	$shift1, in, tmp			\
	VPXOR	out, tmp, out				\
	VPSLLD	$(32-shift1), in, tmp			\
	VPXOR	out, tmp, out				\
	VPSRLD	$shift2, in, tmp			\
	VPXOR	out, tmp, out

// Implementation of the uppercase Sigma function.
#define USIGMA(in, shift0, shift1, shift2, out, tmp) \
	LSIGMA(in, shift0, shift1, shift2, out, tmp)	\
	VPSLLD	$(32-shift2), in, tmp			\
	VPXOR	out, tmp, out

// Perform a single round of SHA-256's compression function.
#define ROUND(a, b, c, d, e, f, g, h, round) 		\
	/* T1 = Sigma1(e) */				\
	USIGMA(e, 6, 11, 25, Y15, Y8)			\
	/* T1 += Ch(e, f, g) = ((f ^ g) & e) ^ g */	\
	VPXOR		f, g, Y8			\
	VPAND		Y8, e, Y8			\
	VPXOR		Y8, g, Y8			\
	VPADDD		Y15, Y8, Y15			\
	/* T1 += h */					\
	VPADDD		Y15, h, Y15			\
	/* T1 += Kt */					\
	VPBROADCASTD 	(round*4)(DX), Y8		\
	VPADDD		Y15, Y8, Y15			\
	/* T1 += Wt */					\
	VPADDD		(round*32)(DI), Y15, Y15	\
	/* T2 = Sigma0(a) */				\
	USIGMA(a, 2, 13, 22, Y14, Y8)			\
	/* T2 += Maj(a, b, c) */			\
	VPAND		a, b, Y8			\
	VPAND		a, c, Y9			\
	VPAND		b, c, Y10			\
	VPXOR		Y8, Y9, Y11			\
	VPXOR		Y10, Y11, Y11			\
	VPADDD		Y14, Y11, Y14			\
	/* d += T1 */					\
	VPADDD		d, Y15, d			\
	/* h = T1 + T2 */				\
	VPADDD		Y14, Y15, h

// Compute W[i], using W[i-2], W[i-7] and W[i-15]. This implementation
// clobbers W[i-7] and the tmp vector registers.
#define EXTEND(wi, wis2, wis7, wis15, tmp, index) \
	VPADDD	wi, wis7, wi			\
	LSIGMA(wis2, 17, 19, 10, wis7, tmp)	\
	VPADDD	wi, wis7, wi			\
	LSIGMA(wis15, 7, 18, 3, wis7, tmp)	\
	VPADDD	wi, wis7, wi			\
	VMOVDQU	wi, (index*32)(DI)

// Apply 16 rounds of SHA-256's compressor function.
#define APPLY_ALL_ROUNDS \
	MOVW	$0, BX						\
	LEAQ	k<>+0(SB), DX					\
more_rounds:							\
	/* Apply 16 rounds. */					\
	ROUND(Y0, Y1, Y2, Y3, Y4, Y5, Y6, Y7, 0)		\
	ROUND(Y7, Y0, Y1, Y2, Y3, Y4, Y5, Y6, 1)		\
	ROUND(Y6, Y7, Y0, Y1, Y2, Y3, Y4, Y5, 2)		\
	ROUND(Y5, Y6, Y7, Y0, Y1, Y2, Y3, Y4, 3)		\
	ROUND(Y4, Y5, Y6, Y7, Y0, Y1, Y2, Y3, 4)		\
	ROUND(Y3, Y4, Y5, Y6, Y7, Y0, Y1, Y2, 5)		\
	ROUND(Y2, Y3, Y4, Y5, Y6, Y7, Y0, Y1, 6)		\
	ROUND(Y1, Y2, Y3, Y4, Y5, Y6, Y7, Y0, 7)		\
	ROUND(Y0, Y1, Y2, Y3, Y4, Y5, Y6, Y7, 8)		\
	ROUND(Y7, Y0, Y1, Y2, Y3, Y4, Y5, Y6, 9)		\
	ROUND(Y6, Y7, Y0, Y1, Y2, Y3, Y4, Y5, 10)		\
	ROUND(Y5, Y6, Y7, Y0, Y1, Y2, Y3, Y4, 11)		\
	ROUND(Y4, Y5, Y6, Y7, Y0, Y1, Y2, Y3, 12)		\
	ROUND(Y3, Y4, Y5, Y6, Y7, Y0, Y1, Y2, 13)		\
	ROUND(Y2, Y3, Y4, Y5, Y6, Y7, Y0, Y1, 14)		\
	ROUND(Y1, Y2, Y3, Y4, Y5, Y6, Y7, Y0, 15)		\
								\
	/* Terminate after 64 rounds. */			\
	INCW	BX						\
	CMPW	BX, $4						\
	JEQ	enough_rounds					\
								\
	/*							\
	 * Temporarily move the hash variables onto the		\
	 * stack to free up registers for the message		\
	 * extension.						\
	 */							\
	VMOVDQU	Y0, 512(DI)					\
	VMOVDQU	Y1, 512+(32*1)(DI)				\
	VMOVDQU	Y2, 512+(32*2)(DI)				\
	VMOVDQU	Y3, 512+(32*3)(DI)				\
	VMOVDQU	Y4, 512+(32*4)(DI)				\
	VMOVDQU	Y5, 512+(32*5)(DI)				\
	VMOVDQU	Y6, 512+(32*6)(DI)				\
	VMOVDQU	Y7, 512+(32*7)(DI)				\
								\
	/*							\
	 * Extend the message, so that we can perform 16	\
	 * more rounds.						\
	 *							\
	 * Due to the mixing that needs to be performed,	\
	 * we'd ideally keep the entire message in		\
	 * registers. Unfortunately, this leaves us with no	\
	 * scratch registers. During each round we'll		\
	 * clobber Wi-8 and Wi-7, meaning that we need to	\
	 * reload those.					\
	 */							\
	VMOVDQU	(DI), Y0					\
	VMOVDQU	(32*1)(DI), Y1					\
	VMOVDQU	(32*2)(DI), Y2					\
	VMOVDQU	(32*3)(DI), Y3					\
	VMOVDQU	(32*4)(DI), Y4					\
	VMOVDQU	(32*5)(DI), Y5					\
	VMOVDQU	(32*6)(DI), Y6					\
	VMOVDQU	(32*7)(DI), Y7					\
	/* Y8 is a scratch register initially. */		\
	VMOVDQU	(32*9)(DI), Y9					\
	VMOVDQU	(32*10)(DI), Y10				\
	VMOVDQU	(32*11)(DI), Y11				\
	VMOVDQU	(32*12)(DI), Y12				\
	VMOVDQU	(32*13)(DI), Y13				\
	VMOVDQU	(32*14)(DI), Y14				\
	VMOVDQU	(32*15)(DI), Y15				\
	EXTEND(Y0, Y14, Y9, Y1, Y8, 0)				\
	VMOVDQU	(32*8)(DI), Y8					\
	EXTEND(Y1, Y15, Y10, Y2, Y9, 1)				\
	VMOVDQU	(32*9)(DI), Y9					\
	EXTEND(Y2, Y0, Y11, Y3, Y10, 2)				\
	VMOVDQU	(32*10)(DI), Y10				\
	EXTEND(Y3, Y1, Y12, Y4, Y11, 3)				\
	VMOVDQU	(32*11)(DI), Y11				\
	EXTEND(Y4, Y2, Y13, Y5, Y12, 4)				\
	VMOVDQU	(32*12)(DI), Y12				\
	EXTEND(Y5, Y3, Y14, Y6, Y13, 5)				\
	VMOVDQU	(32*13)(DI), Y13				\
	EXTEND(Y6, Y4, Y15, Y7, Y14, 6)				\
	VMOVDQU	(32*14)(DI), Y14				\
	EXTEND(Y7, Y5, Y0, Y8, Y15, 7)				\
	VMOVDQU	(32*15)(DI), Y15				\
	EXTEND(Y8, Y6, Y1, Y9, Y0, 8)				\
	VMOVDQU	(DI), Y0					\
	EXTEND(Y9, Y7, Y2, Y10, Y1, 9)				\
	EXTEND(Y10, Y8, Y3, Y11, Y2, 10)			\
	EXTEND(Y11, Y9, Y4, Y12, Y3, 11)			\
	EXTEND(Y12, Y10, Y5, Y13, Y4, 12)			\
	EXTEND(Y13, Y11, Y6, Y14, Y5, 13)			\
	EXTEND(Y14, Y12, Y7, Y15, Y6, 14)			\
	EXTEND(Y15, Y13, Y8, Y0, Y7, 15)			\
								\
	/* Reload the hash variables */				\
	VMOVDQU	512(DI), Y0					\
	VMOVDQU	512+(32*1)(DI), Y1				\
	VMOVDQU	512+(32*2)(DI), Y2				\
	VMOVDQU	512+(32*3)(DI), Y3				\
	VMOVDQU	512+(32*4)(DI), Y4				\
	VMOVDQU	512+(32*5)(DI), Y5				\
	VMOVDQU	512+(32*6)(DI), Y6				\
	VMOVDQU	512+(32*7)(DI), Y7				\
								\
	/* Progress K. */					\
	ADDQ	$64, DX						\
	JMP	more_rounds					\
enough_rounds:

// func hashChunksVectorized(input *[8192]byte, output *[32]uint32)
TEXT ·hashChunksVectorized(SB), $1056-16
	ALIGN_STACK

	// Load initialization vectors.
	VPBROADCASTD iv_data<>+0(SB), Y0
	VPBROADCASTD iv_data<>+4(SB), Y1
	VPBROADCASTD iv_data<>+8(SB), Y2
	VPBROADCASTD iv_data<>+12(SB), Y3
	VPBROADCASTD iv_data<>+16(SB), Y4
	VPBROADCASTD iv_data<>+20(SB), Y5
	VPBROADCASTD iv_data<>+24(SB), Y6
	VPBROADCASTD iv_data<>+28(SB), Y7

	MOVQ	input+0(FP), AX
	MOVW	$0, CX

#define LOAD_BIG_ENDIAN_TRANSPOSE_AND_STORE_8X8(read_offset, stack_offset) \
	/* Read data. */				\
	VMOVDQU		read_offset(AX), Y8		\
	VMOVDQU		(read_offset+1024)(AX), Y9	\
	VMOVDQU		(read_offset+2048)(AX), Y10	\
	VMOVDQU		(read_offset+3072)(AX), Y11	\
	VMOVDQU		(read_offset+4096)(AX), Y12	\
	VMOVDQU		(read_offset+5120)(AX), Y13	\
	VMOVDQU		(read_offset+6144)(AX), Y14	\
	VMOVDQU		(read_offset+7168)(AX), Y15	\
	/* Convert byte order. */			\
	VMOVDQU		endian<>+0(SB), Y0		\
	VPSHUFB		Y0, Y8, Y8			\
	VPSHUFB		Y0, Y9, Y9			\
	VPSHUFB		Y0, Y10, Y10			\
	VPSHUFB		Y0, Y11, Y11			\
	VPSHUFB		Y0, Y12, Y12			\
	VPSHUFB		Y0, Y13, Y13			\
	VPSHUFB		Y0, Y14, Y14			\
	VPSHUFB		Y0, Y15, Y15			\
	/* Rearrange double words. */			\
	VPUNPCKLDQ	Y9, Y8, Y0			\
	VPUNPCKHDQ	Y9, Y8, Y8			\
	VPUNPCKLDQ	Y11, Y10, Y9			\
	VPUNPCKHDQ	Y11, Y10, Y10			\
	VPUNPCKLDQ	Y13, Y12, Y11			\
	VPUNPCKHDQ	Y13, Y12, Y12			\
	VPUNPCKLDQ	Y15, Y14, Y13			\
	VPUNPCKHDQ	Y15, Y14, Y14			\
	/* Rearrange quad words. */			\
	VPUNPCKLQDQ	Y9, Y0, Y15			\
	VPUNPCKHQDQ	Y9, Y0, Y0			\
	VPUNPCKLQDQ	Y10, Y8, Y9			\
	VPUNPCKHQDQ	Y10, Y8, Y8			\
	VPUNPCKLQDQ	Y13, Y11, Y10			\
	VPUNPCKHQDQ	Y13, Y11, Y11			\
	VPUNPCKLQDQ	Y14, Y12, Y13			\
	VPUNPCKHQDQ	Y14, Y12, Y12			\
	/* Rearrange halves. */				\
	VINSERTI128	$0x01, X10, Y15, Y14		\
	VPERM2I128	$0x31, Y10, Y15, Y10		\
	VINSERTI128	$0x01, X11, Y0, Y15		\
	VPERM2I128	$0x31, Y11, Y0, Y0		\
	VINSERTI128	$0x01, X13, Y9, Y11		\
	VPERM2I128	$0x31, Y13, Y9, Y9		\
	VINSERTI128	$0x01, X12, Y8, Y13		\
	VPERM2I128	$0x31, Y12, Y8, Y8		\
	/* Store data. */				\
	VMOVDQU		Y14, stack_offset(DI)		\
	VMOVDQU		Y15, (stack_offset+32*1)(DI)	\
	VMOVDQU		Y11, (stack_offset+32*2)(DI)	\
	VMOVDQU		Y13, (stack_offset+32*3)(DI)	\
	VMOVDQU		Y10, (stack_offset+32*4)(DI)	\
	VMOVDQU		Y0, (stack_offset+32*5)(DI)	\
	VMOVDQU		Y9, (stack_offset+32*6)(DI)	\
	VMOVDQU		Y8, (stack_offset+32*7)(DI)

block:
	// Save H, as we need to add the values of a to h to them at the
	// end of the hash computation.
	VMOVDQU	Y0, 768(DI)
	VMOVDQU	Y1, 768+(32*1)(DI)
	VMOVDQU	Y2, 768+(32*2)(DI)
	VMOVDQU	Y3, 768+(32*3)(DI)
	VMOVDQU	Y4, 768+(32*4)(DI)
	VMOVDQU	Y5, 768+(32*5)(DI)
	VMOVDQU	Y6, 768+(32*6)(DI)
	VMOVDQU	Y7, 768+(32*7)(DI)

	// If we've hashed all 16 blocks, don't load more input data.
	// Instead, add a 17th block that contains the Merkle-Damgard
	// padding.
	CMPW	CX, $16
	JEQ	block_padding

	// Load 512 bits of input from each of the chunks, and transpose
	// it. As we can't do this using exactly 8 registers, we need to
	// reload Y0 after we're done.
	LOAD_BIG_ENDIAN_TRANSPOSE_AND_STORE_8X8(0, 0)
	LOAD_BIG_ENDIAN_TRANSPOSE_AND_STORE_8X8(32, 256)
	VMOVDQA	768(DI), Y0

block_rounds:
	APPLY_ALL_ROUNDS

	// Add the original values of H to the results.
	VPADDD	768(DI), Y0, Y0
	VPADDD	768+(32*1)(DI), Y1, Y1
	VPADDD	768+(32*2)(DI), Y2, Y2
	VPADDD	768+(32*3)(DI), Y3, Y3
	VPADDD	768+(32*4)(DI), Y4, Y4
	VPADDD	768+(32*5)(DI), Y5, Y5
	VPADDD	768+(32*6)(DI), Y6, Y6
	VPADDD	768+(32*7)(DI), Y7, Y7

	// Progress to the next block in each chunk.
	ADDQ	$64, AX
	INCW	CX
	CMPW	CX, $16
	JG	chunk_done
	JMP	block

block_padding:
	// Already processed 16 blocks of data. Add a 17th
	// Merkle-Damgard padding block, to terminate the chunk.
	VPBROADCASTD	padding<>+0(SB), Y8
	VMOVDQU		Y8, (DI)
	VPBROADCASTD	padding<>+4(SB), Y8
	VMOVDQU		Y8, (32*1)(DI)
	VMOVDQU		Y8, (32*2)(DI)
	VMOVDQU		Y8, (32*3)(DI)
	VMOVDQU		Y8, (32*4)(DI)
	VMOVDQU		Y8, (32*5)(DI)
	VMOVDQU		Y8, (32*6)(DI)
	VMOVDQU		Y8, (32*7)(DI)
	VMOVDQU		Y8, (32*8)(DI)
	VMOVDQU		Y8, (32*9)(DI)
	VMOVDQU		Y8, (32*10)(DI)
	VMOVDQU		Y8, (32*11)(DI)
	VMOVDQU		Y8, (32*12)(DI)
	VMOVDQU		Y8, (32*13)(DI)
	VMOVDQU		Y8, (32*14)(DI)
	VPBROADCASTD	padding<>+8(SB), Y8
	VMOVDQU		Y8, (32*15)(DI)

	JMP block_rounds

chunk_done:
	// Store resulting hash.
	MOVQ	output+8(FP), AX
	VMOVDQU	Y0, (AX)
	VMOVDQU	Y1, 32*1(AX)
	VMOVDQU	Y2, 32*2(AX)
	VMOVDQU	Y3, 32*3(AX)
	VMOVDQU	Y4, 32*4(AX)
	VMOVDQU	Y5, 32*5(AX)
	VMOVDQU	Y6, 32*6(AX)
	VMOVDQU	Y7, 32*7(AX)
	VZEROUPPER
	RET

// func hashParentsVectorized(left *[32]uint32, right *[32]uint32, out *[32]uint32)
TEXT ·hashParentsVectorized(SB), $800-24
	ALIGN_STACK

	// Load initialization vectors.
	VPBROADCASTD	iv_parent<>+0(SB), Y0
	VPBROADCASTD	iv_parent<>+4(SB), Y1
	VPBROADCASTD	iv_parent<>+8(SB), Y2
	VPBROADCASTD	iv_parent<>+12(SB), Y3
	VPBROADCASTD	iv_parent<>+16(SB), Y4
	VPBROADCASTD	iv_parent<>+20(SB), Y5
	VPBROADCASTD	iv_parent<>+24(SB), Y6
	VPBROADCASTD	iv_parent<>+28(SB), Y7

#define COPY_CHILD(in_offset, out_offset) \
	MOVQ		in_offset(FP), AX		\
	VMOVDQU		(AX), Y8			\
	VMOVDQU		32*1(AX), Y9			\
	VMOVDQU		32*2(AX), Y10			\
	VMOVDQU		32*3(AX), Y11			\
	VMOVDQU		32*4(AX), Y12			\
	VMOVDQU		32*5(AX), Y13			\
	VMOVDQU		32*6(AX), Y14			\
	VMOVDQU		32*7(AX), Y15			\
	VMOVDQU		Y8, out_offset(DI)		\
	VMOVDQU		Y9, out_offset+32*1(DI)		\
	VMOVDQU		Y10, out_offset+32*2(DI)	\
	VMOVDQU		Y11, out_offset+32*3(DI)	\
	VMOVDQU		Y12, out_offset+32*4(DI)	\
	VMOVDQU		Y13, out_offset+32*5(DI)	\
	VMOVDQU		Y14, out_offset+32*6(DI)	\
	VMOVDQU		Y15, out_offset+32*7(DI)

	// Concatenate left and right children.
	COPY_CHILD(left+0, 0)
	COPY_CHILD(right+8, 256)

	// Perform hashing.
	APPLY_ALL_ROUNDS

	// Store resulting hash.
	MOVQ	output+16(FP), AX
	VMOVDQU	Y0, (AX)
	VMOVDQU	Y1, 32*1(AX)
	VMOVDQU	Y2, 32*2(AX)
	VMOVDQU	Y3, 32*3(AX)
	VMOVDQU	Y4, 32*4(AX)
	VMOVDQU	Y5, 32*5(AX)
	VMOVDQU	Y6, 32*6(AX)
	VMOVDQU	Y7, 32*7(AX)
	VZEROUPPER
	RET
