//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
)

type convertedInvertedCompress struct {
	c1 *segmentCursorInvertedReusable

	w    io.WriteSeeker
	bufw *bufio.Writer

	scratchSpacePath string

	offset         int
	invertedHeader *segmentindex.HeaderInverted

	inEncoders  []varenc.VarEncEncoder[uint64]
	outEncoders []varenc.VarEncEncoder[uint64]

	outTypes []varenc.VarEncDataType
}

func newConvertedInvertedCompress(w io.WriteSeeker,
	c1 *segmentCursorInvertedReusable, scratchSpacePath string, types []varenc.VarEncDataType,
) *convertedInvertedCompress {
	return &convertedInvertedCompress{
		c1:               c1,
		w:                w,
		bufw:             bufio.NewWriterSize(w, 256*1024),
		scratchSpacePath: scratchSpacePath,
		offset:           0,
		outTypes:         types,
	}
}

func (c *convertedInvertedCompress) do() error {
	var err error

	if err := c.init(); err != nil {
		return errors.Wrap(err, "init")
	}

	kis, err := c.writeKeys()
	if err != nil {
		return errors.Wrap(err, "write keys")
	}

	tombstoneOffset := c.offset
	_, err = c.writeTombstones()
	if err != nil {
		return errors.Wrap(err, "write tombstones")
	}

	propertyLengthsOffset := c.offset
	_, err = c.writePropertyLengths()
	if err != nil {
		return errors.Wrap(err, "write property lengths")
	}
	treeOffset := uint64(c.offset)
	if err := c.writeIndices(kis); err != nil {
		return errors.Wrap(err, "write index")
	}

	// flush buffered, so we can safely seek on underlying writer
	if err := c.bufw.Flush(); err != nil {
		return errors.Wrap(err, "flush buffered")
	}
	if err := c.writeHeader(c.c1.segment.level, 0, c.c1.segment.secondaryIndexCount,
		treeOffset); err != nil {
		return errors.Wrap(err, "write header")
	}

	if err := c.writeInvertedHeader(tombstoneOffset, propertyLengthsOffset); err != nil {
		return errors.Wrap(err, "write keys length")
	}

	return nil
}

func (c *convertedInvertedCompress) init() error {
	// write a dummy header, we don't know the contents of the actual header yet,
	// we will seek to the beginning and overwrite the actual header at the very
	// end
	if _, err := c.bufw.Write(make([]byte, segmentindex.HeaderSize)); err != nil {
		return errors.Wrap(err, "write empty header")
	}

	if _, err := c.bufw.Write(make([]byte, segmentindex.SegmentInvertedDefaultHeaderSize+len(c.outTypes))); err != nil {
		return errors.Wrap(err, "write empty inverted header")
	}
	c.offset = segmentindex.HeaderSize + segmentindex.SegmentInvertedDefaultHeaderSize + len(c.outTypes)

	c.invertedHeader = &segmentindex.HeaderInverted{
		KeysOffset:            uint64(c.offset),
		TombstoneOffset:       0,
		PropertyLengthsOffset: 0,
		Version:               0,
		BlockSize:             uint8(segmentindex.SegmentInvertedDefaultBlockSize),
		DataFieldCount:        uint8(len(c.outTypes)),
		DataFields:            c.outTypes,
	}

	c.inEncoders = make([]varenc.VarEncEncoder[uint64], 0, len(c.outTypes))
	c.outEncoders = make([]varenc.VarEncEncoder[uint64], 0, len(c.outTypes))

	for i, df := range c.c1.segment.invertedHeader.DataFields {
		c.inEncoders = append(c.inEncoders, varenc.GetVarEncEncoder64(df))
		c.inEncoders[i].Init(terms.BLOCK_SIZE)
	}

	for i, df := range c.invertedHeader.DataFields {
		c.outEncoders = append(c.outEncoders, varenc.GetVarEncEncoder64(df))
		c.outEncoders[i].Init(terms.BLOCK_SIZE)
	}

	return nil
}

func (c *convertedInvertedCompress) writeTombstones() (int, error) {
	tombstonesBuffer := make([]byte, 0)

	tombstones, err := c.c1.segment.loadTombstones()
	if err != nil {
		return 0, err
	}

	if tombstones != nil && tombstones.GetCardinality() > 0 {
		tombstonesBuffer = tombstones.ToBuffer()
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(len(tombstonesBuffer)))
	if _, err := c.bufw.Write(buf); err != nil {
		return 0, err
	}

	if _, err := c.bufw.Write(tombstonesBuffer); err != nil {
		return 0, err
	}
	c.offset += len(tombstonesBuffer) + 8
	return len(tombstonesBuffer) + 8, nil
}

func (c *convertedInvertedCompress) writePropertyLengths() (int, error) {
	propLengths, err := c.c1.segment.loadPropertyLengths()
	if err != nil {
		return 0, err
	}

	b := new(bytes.Buffer)

	e := gob.NewEncoder(b)

	// Encoding the map
	err = e.Encode(propLengths)
	if err != nil {
		return 0, err
	}

	buf := make([]byte, 8)

	binary.LittleEndian.PutUint64(buf, c.c1.segment.invertedData.avgPropertyLengthsSum)
	if _, err := c.bufw.Write(buf); err != nil {
		return 0, err
	}

	binary.LittleEndian.PutUint64(buf, c.c1.segment.invertedData.avgPropertyLengthsCount)
	if _, err := c.bufw.Write(buf); err != nil {
		return 0, err
	}

	binary.LittleEndian.PutUint64(buf, uint64(b.Len()))
	if _, err := c.bufw.Write(buf); err != nil {
		return 0, err
	}

	if _, err := c.bufw.Write(b.Bytes()); err != nil {
		return 0, err
	}
	c.offset += b.Len() + 8 + 8 + 8
	return b.Len() + 8 + 8 + 8, nil
}

func (c *convertedInvertedCompress) writeKeys() ([]segmentindex.Key, error) {
	key, value, _ := c.c1.firstNoDecode()

	// the (dummy) header was already written, this is our initial offset

	var kis []segmentindex.Key

	for {
		if key == nil {
			break
		}

		ki, err := c.writeIndividualNode(c.offset, key, value)
		if err != nil {
			return nil, errors.Wrap(err, "write individual node (key1 smaller)")
		}

		c.offset = ki.ValueEnd
		kis = append(kis, ki)

		key, value, _ = c.c1.nextNoDecode()

	}

	return kis, nil
}

func (c *convertedInvertedCompress) writeIndividualNode(offset int, key, values []byte) (segmentindex.Key, error) {
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	out := segmentindex.Key{}
	written := 0
	buf := make([]byte, 8) // uint64 size

	blocksEncoded := convertBlocks(values, terms.ENCODE_AS_FULL_BYTES, c.inEncoders, c.outEncoders)
	n, err := c.bufw.Write(blocksEncoded)
	if err != nil {
		return out, errors.Wrapf(err, "write values for node")
	}
	written += n

	keyLength := uint32(len(keyCopy))
	binary.LittleEndian.PutUint32(buf[0:4], keyLength)
	if _, err := c.bufw.Write(buf[0:4]); err != nil {
		return out, errors.Wrapf(err, "write key length encoding for node")
	}
	written += 4

	n, err = c.bufw.Write(keyCopy)
	if err != nil {
		return out, errors.Wrapf(err, "write node")
	}
	written += n

	out = segmentindex.Key{
		ValueStart: offset,
		ValueEnd:   offset + written,
		Key:        keyCopy,
	}

	return out, nil
}

func (c *convertedInvertedCompress) writeIndices(keys []segmentindex.Key) error {
	indices := segmentindex.Indexes{
		Keys:                keys,
		SecondaryIndexCount: c.c1.segment.secondaryIndexCount,
		ScratchSpacePath:    c.scratchSpacePath,
	}

	_, err := indices.WriteTo(c.bufw)
	return err
}

// writeHeader assumes that everything has been written to the underlying
// writer and it is now safe to seek to the beginning and override the initial
// header
func (c *convertedInvertedCompress) writeHeader(level, version, secondaryIndices uint16,
	startOfIndex uint64,
) error {
	if _, err := c.w.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "seek to beginning to write header")
	}

	h := &segmentindex.Header{
		Level:            level,
		Version:          version,
		SecondaryIndices: secondaryIndices,
		Strategy:         segmentindex.StrategyInverted,
		IndexStart:       startOfIndex,
	}

	if _, err := h.WriteTo(c.w); err != nil {
		return err
	}

	return nil
}

// writeInvertedHeader assumes that everything has been written to the underlying
// writer and it is now safe to seek to the beginning and override the initial
// header
func (c *convertedInvertedCompress) writeInvertedHeader(tombstoneOffset, propertyLengthsOffset int) error {
	if _, err := c.w.Seek(16, io.SeekStart); err != nil {
		return errors.Wrap(err, "seek to beginning to write inverted header")
	}

	c.invertedHeader.TombstoneOffset = uint64(tombstoneOffset)
	c.invertedHeader.PropertyLengthsOffset = uint64(propertyLengthsOffset)

	if _, err := c.invertedHeader.WriteTo(c.w); err != nil {
		return err
	}

	return nil
}
