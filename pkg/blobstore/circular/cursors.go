package circular

// Cursors is a pair of offsets within the data file, indicating which
// part of the file contains valid readable data and where future writes
// need to take place.
type Cursors struct {
	Read  uint64
	Write uint64
}

// Contains returns whether the provided offset and length are contained
// with the cursors. In effect, it returns whether the offset/length
// pair still corresponds to valid data.
func (c *Cursors) Contains(offset uint64, length int64) bool {
	if length < 1 {
		length = 1
	}
	return offset >= c.Read && offset <= c.Write && offset+uint64(length) <= c.Write
}
