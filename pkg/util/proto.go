package util

import (
	"bufio"
	"io"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

// ProtoBytesFieldVisitor is the callback type that is used by
// VisitProtoBytesFields to report individual fields.
type ProtoBytesFieldVisitor func(fieldNumber protowire.Number, offsetBytes, sizeBytes int64, fieldReader io.Reader) error

type protoBytesFieldReader struct {
	reader             *bufio.Reader
	remainingSizeBytes uint64
}

func (r *protoBytesFieldReader) Read(p []byte) (int, error) {
	if r.remainingSizeBytes == 0 {
		return 0, io.EOF
	}
	if uint64(len(p)) > r.remainingSizeBytes {
		p = p[:r.remainingSizeBytes]
	}
	n, err := r.reader.Read(p)
	r.remainingSizeBytes -= uint64(n)
	if err == io.EOF && r.remainingSizeBytes > 0 {
		err = status.Errorf(codes.InvalidArgument, "Field ended prematurely, as %d more bytes were expected", r.remainingSizeBytes)
	}
	return n, err
}

// VisitProtoBytesFields reads a marshaled Protobuf message from a
// Reader and calls into ProtoBytesFieldVisitor for any top-level field
// that it encounters.
//
// As the name suggests, this function is only capable of parsing
// message fields that use wire type 2, which is used for bytes,
// strings, embedded messages, and packed repeated fields.
func VisitProtoBytesFields(r io.Reader, visitor ProtoBytesFieldVisitor) error {
	offsetBytes := uint64(0)
	br := bufio.NewReader(r)
	for {
		// Peek the field's tag and size.
		header, err := br.Peek(32)
		if err != nil {
			if err != io.EOF {
				return err
			} else if len(header) == 0 {
				return nil
			}
		}

		// Parse the field's tag.
		fieldNumber, fieldType, nTag := protowire.ConsumeTag(header)
		if nTag < 0 {
			return StatusWrapfWithCode(protowire.ParseError(nTag), codes.InvalidArgument, "Field at offset %d has an invalid tag", offsetBytes)
		}
		if fieldType != protowire.BytesType {
			return status.Errorf(codes.InvalidArgument, "Field with number %d at offset %d has type %d, while %d was expected", fieldNumber, offsetBytes, fieldType, protowire.BytesType)
		}

		// Parse the field's size.
		fieldSizeBytes, nLength := protowire.ConsumeVarint(header[nTag:])
		if nLength < 0 {
			return StatusWrapfWithCode(protowire.ParseError(nLength), codes.InvalidArgument, "Field with number %d at offset %d has an invalid size", fieldNumber, offsetBytes)
		}
		if fieldSizeBytes > math.MaxInt64-offsetBytes {
			return status.Errorf(codes.InvalidArgument, "Field with number %d at offset %d has size %d, which is too large", fieldNumber, offsetBytes, fieldSizeBytes)
		}

		nHeader := nTag + nLength
		if _, err := br.Discard(nHeader); err != nil {
			return err
		}
		offsetBytes += uint64(nHeader)

		// Call into the visitor.
		fieldReader := protoBytesFieldReader{
			reader:             br,
			remainingSizeBytes: fieldSizeBytes,
		}
		if err := visitor(fieldNumber, int64(offsetBytes), int64(fieldSizeBytes), &fieldReader); err != nil {
			return StatusWrapf(err, "Field with number %d at offset %d size %d", fieldNumber, offsetBytes, fieldSizeBytes)
		}

		// Discard the part of the field that wasn't read by the
		// visitor, so that we read the next tag from the right
		// offset.
		remainingSizeBytes := int(fieldReader.remainingSizeBytes)
		if n, err := br.Discard(remainingSizeBytes); err == io.EOF {
			return status.Errorf(codes.InvalidArgument, "Field with number %d at offset %d size %d ended prematurely, as %d more bytes were expected", fieldNumber, offsetBytes, fieldSizeBytes, remainingSizeBytes-n)
		} else if err != nil {
			return err
		}
		offsetBytes += fieldSizeBytes
	}
}
