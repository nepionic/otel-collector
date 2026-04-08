// Package adsbridge provides ring buffer parsing helpers for the sequence-tagged
// SPSC ring buffers published by the TwinCAT OtelBridge companion library.
package adsbridge

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Metric ring buffer
// ---------------------------------------------------------------------------
//
// TwinCAT struct layout (OtelMetricEntry — 672 bytes, little-endian):
//
//   offset   0  ULINT   timestamp         Unix epoch nanoseconds
//   offset   8  UDINT   kind              0=Gauge 1=Counter 2=UpDownCounter
//   offset  12  UDINT   attr_count        Number of populated attrs (0..5)
//   offset  16  BYTE[128] name            STRING(127) null-terminated
//   offset 144  BYTE[32]  unit            STRING(31)  null-terminated
//   offset 176  LREAL   value             Metric value (float64)
//   offset 184  MetricAttribute[5]        5 × 96 bytes = 480 bytes
//                                           key  STRING(31) = 32 bytes
//                                           value STRING(63) = 64 bytes
//   offset 664  BYTE[4]   _pad
//   offset 668  UDINT   sequence          ODD=being written, EVEN=committed (LAST field)
//   Total: 672 bytes
//
// Ring header (64 bytes): identical layout to the log ring header.
// Ring: header (64 bytes) + OtelMetricEntry[256] (172032 bytes) = 172096 bytes total.

const (
	MetricHeaderSize = 64
	MetricSlotSize   = 672

	// MetricKind values match the PLC MetricKind enum.
	MetricKindGauge         uint32 = 0
	MetricKindCounter       uint32 = 1
	MetricKindUpDownCounter uint32 = 2

	MetricAttrCount   = 5
	MetricAttrKeySize = 32 // STRING(31) + null
	MetricAttrValSize = 64 // STRING(63) + null
)

// MetricAttr is a single structured key/value label from a metric slot.
type MetricAttr struct {
	Key   string
	Value string
}

// MetricSlot is a parsed metric ring buffer entry.
type MetricSlot struct {
	TimestampNs uint64
	Kind        uint32
	AttrCount   uint32
	Name        string
	Unit        string
	ValueF64    float64
	Attrs       []MetricAttr
}

// RingHeader is the parsed header from the first 64 bytes of a ring buffer.
type RingHeader struct {
	WriteIndex  uint32
	Capacity    uint32
	OverflowCnt uint32
}

// ParseMetricRingHeader parses the 64-byte header from a raw buffer read.
func ParseMetricRingHeader(b []byte) (RingHeader, error) {
	if len(b) < MetricHeaderSize {
		return RingHeader{}, fmt.Errorf("metric ring header: need %d bytes, got %d", MetricHeaderSize, len(b))
	}
	return RingHeader{
		WriteIndex:  binary.LittleEndian.Uint32(b[0:]),
		Capacity:    binary.LittleEndian.Uint32(b[4:]),
		OverflowCnt: binary.LittleEndian.Uint32(b[8:]),
	}, nil
}

// ParseMetricSlot deserialises a 672-byte raw metric slot.
func ParseMetricSlot(b []byte) (MetricSlot, error) {
	if len(b) < MetricSlotSize {
		return MetricSlot{}, fmt.Errorf("metric slot: need %d bytes, got %d", MetricSlotSize, len(b))
	}

	attrCount := binary.LittleEndian.Uint32(b[12:])
	if attrCount > MetricAttrCount {
		attrCount = MetricAttrCount
	}

	attrs := make([]MetricAttr, 0, attrCount)
	for i := uint32(0); i < attrCount; i++ {
		base := 184 + int(i)*(MetricAttrKeySize+MetricAttrValSize)
		attrs = append(attrs, MetricAttr{
			Key:   nullTermString(b[base : base+MetricAttrKeySize]),
			Value: nullTermString(b[base+MetricAttrKeySize : base+MetricAttrKeySize+MetricAttrValSize]),
		})
	}

	return MetricSlot{
		TimestampNs: binary.LittleEndian.Uint64(b[0:]),
		Kind:        binary.LittleEndian.Uint32(b[8:]),
		AttrCount:   attrCount,
		Name:        nullTermString(b[16:144]),
		Unit:        nullTermString(b[144:176]),
		ValueF64:    math.Float64frombits(binary.LittleEndian.Uint64(b[176:])),
		Attrs:       attrs,
		// sequence is at b[668:672] — validated by DrainMetrics.
	}, nil
}

// TimestampNsToTime converts a PLC Unix-epoch nanosecond timestamp to time.Time.
// Returns time.Now() if the PLC timestamp is zero (uninitialised).
func TimestampNsToTime(ns uint64) time.Time {
	if ns == 0 {
		return time.Now()
	}
	return time.Unix(0, int64(ns))
}

// ---------------------------------------------------------------------------
// DrainMetrics – stateless, call with each new write_index
// ---------------------------------------------------------------------------

// DrainResult holds the output of a single drain call.
type DrainResult struct {
	Slots       []MetricSlot
	TornReads   uint32 // slots skipped due to odd sequence (mid-write during ADS read)
	Overflows   uint32 // slots skipped due to generation mismatch (ring lapped consumer)
	OverflowCnt uint32 // overflow_cnt value from ring header
}

// DrainMetrics parses new metric slots from a raw ring buffer snapshot.
//
// rawRing is the full ring buffer bytes (header + all slots) read with ReadRaw.
// localRI is the consumer's current read index (maintained by the caller using
// atomic.LoadUint32 / atomic.StoreUint32 so it is safe for concurrent reads).
// newWI is the write_index value from the latest ADS notification.
//
// Returns the valid slots and an updated localRI to store back.
func DrainMetrics(rawRing []byte, localRI uint32, newWI uint32) (DrainResult, uint32) {
	var result DrainResult

	if newWI == localRI {
		return result, localRI
	}

	// Parse header first to get capacity and overflow count.
	header, err := ParseMetricRingHeader(rawRing)
	if err != nil {
		return result, localRI
	}
	result.OverflowCnt = header.OverflowCnt

	capacity := header.Capacity
	if capacity == 0 {
		capacity = 2048 // sane fallback
	}

	// Detect consumer falling too far behind (ring has lapped us).
	pendingCount := newWI - localRI
	if pendingCount > capacity {
		// We lost data. Fast-forward to keep up with the ring.
		lost := pendingCount - capacity/2
		localRI += lost
		pendingCount = capacity / 2
		result.Overflows += lost
	}

	result.Slots = make([]MetricSlot, 0, pendingCount)

	for i := uint32(0); i < pendingCount; i++ {
		absIdx := localRI + i
		slotIdx := absIdx % capacity

		slotOff := MetricHeaderSize + int(slotIdx)*MetricSlotSize
		if slotOff+MetricSlotSize > len(rawRing) {
			break
		}
		raw := rawRing[slotOff : slotOff+MetricSlotSize]

		// Validate sequence: even generation means committed.
		seq := binary.LittleEndian.Uint32(raw[668:])
		expectedGen := ((absIdx / capacity) + 1) * 2

		switch {
		case seq == expectedGen:
			slot, err := ParseMetricSlot(raw)
			if err == nil {
				result.Slots = append(result.Slots, slot)
			}
		case seq%2 == 1:
			// PLC was mid-write during our ADS read. Rare. Drop this record.
			result.TornReads++
		default:
			// Generation mismatch – ring wrapped past us faster than expected.
			result.Overflows++
		}
	}

	return result, newWI
}

// ---------------------------------------------------------------------------
// Consumer read-index – thread-safe uint32 helpers
// ---------------------------------------------------------------------------

// ReadIndexStore wraps a uint32 for atomic access by the drain goroutine.
type ReadIndexStore struct {
	v uint32 // aligned for atomic ops
}

func (r *ReadIndexStore) Load() uint32   { return atomic.LoadUint32(&r.v) }
func (r *ReadIndexStore) Store(v uint32) { atomic.StoreUint32(&r.v, v) }
func (r *ReadIndexStore) CompareAndSwap(old, new uint32) bool {
	return atomic.CompareAndSwapUint32(&r.v, old, new)
}

// ---------------------------------------------------------------------------
// Log ring buffer
// ---------------------------------------------------------------------------
//
// TwinCAT struct layout (OtelLogEntry — 1280 bytes, little-endian):
//
//   offset    0  ULINT   timestamp         Unix epoch nanoseconds
//   offset    8  UDINT   severity          0=Debug 1=Info 2=Warning 3=Error 4=Fatal
//   offset   12  UDINT   attr_count        Number of populated attrs (0..10)
//   offset   16  BYTE[192] message         STRING(191) null-terminated
//   offset  208  BYTE[64]  source          STRING(63)  null-terminated (instance path)
//   offset  272  LogAttribute[10]          10 × 96 bytes = 960 bytes
//                  each attr (96 bytes):
//                    offset  0  key         STRING(31)          = 32 bytes
//                    offset 32  attr_type   LogAttributeType    =  1 byte
//                    offset 33  value_bytes ARRAY[0..62] BYTE   = 63 bytes
//   offset 1232  BYTE[44]  _pad
//   offset 1276  UDINT   sequence          ODD=being written, EVEN=committed (LAST field)
//   Total: 1280 bytes
//
// Ring header (64 bytes): identical layout to metric ring header.
// Ring: header (64 bytes) + OtelLogEntry[128] (163840 bytes) = 163904 bytes total.

// LogAttrType mirrors the PLC LogAttributeType enum (USINT-backed).
type LogAttrType uint8

const (
	LogAttrTypeUInt8   LogAttrType = 0  // USINT: 1 byte unsigned
	LogAttrTypeUInt16  LogAttrType = 1  // UINT:  2 bytes LE unsigned
	LogAttrTypeUInt32  LogAttrType = 2  // UDINT: 4 bytes LE unsigned
	LogAttrTypeUInt64  LogAttrType = 3  // ULINT: 8 bytes LE unsigned
	LogAttrTypeInt8    LogAttrType = 4  // SINT:  1 byte signed
	LogAttrTypeInt16   LogAttrType = 5  // INT:   2 bytes LE signed
	LogAttrTypeInt32   LogAttrType = 6  // DINT:  4 bytes LE signed
	LogAttrTypeInt64   LogAttrType = 7  // LINT:  8 bytes LE signed
	LogAttrTypeBoolean LogAttrType = 8  // BOOL:  value_bytes[0]: 0=false, 1=true
	LogAttrTypeFloat32 LogAttrType = 9  // REAL:  4 bytes LE IEEE 754
	LogAttrTypeFloat64 LogAttrType = 10 // LREAL: 8 bytes LE IEEE 754
	LogAttrTypeStr     LogAttrType = 11 // null-terminated STRING in value_bytes
)

const (
	LogHeaderSize  = 64
	LogSlotSize    = 1280
	LogCapacity    = 128
	LogAttrCount   = 10
	LogAttrSize    = 96 // key(32) + attr_type(1) + value_bytes(63)
	LogKeySize     = 32 // STRING(31) + null
	LogAttrTypeOff = 32 // attr_type byte offset within an attr
	LogAttrValOff  = 33 // value_bytes start offset within an attr
	LogAttrValLen  = 63 // value_bytes length
)

// LogAttr is a typed structured attribute from a log entry.
// Only the field corresponding to AttrType is meaningful.
type LogAttr struct {
	Key         string
	AttrType    LogAttrType
	StrValue    string  // AttrTypeString
	BoolValue   bool    // AttrTypeBool
	IntValue    int64   // AttrTypeInt/DInt/LInt/UInt/UDInt/ULInt
	DoubleValue float64 // AttrTypeReal/LReal
}

// LogSlot is a parsed log ring buffer entry.
type LogSlot struct {
	TimestampNs uint64
	Severity    uint32
	AttrCount   uint32
	Message     string
	Source      string
	Attrs       []LogAttr
}

// ParseLogSlot deserialises a 1280-byte raw log slot.
func ParseLogSlot(b []byte) (LogSlot, error) {
	if len(b) < LogSlotSize {
		return LogSlot{}, fmt.Errorf("log slot: need %d bytes, got %d", LogSlotSize, len(b))
	}

	attrCount := binary.LittleEndian.Uint32(b[12:])
	if attrCount > LogAttrCount {
		attrCount = LogAttrCount
	}

	attrs := make([]LogAttr, 0, attrCount)
	for i := uint32(0); i < attrCount; i++ {
		base := 272 + int(i)*LogAttrSize
		key := nullTermString(b[base : base+LogKeySize])
		raw := b[base+LogAttrValOff : base+LogAttrValOff+LogAttrValLen]
		attrType := LogAttrType(b[base+LogAttrTypeOff])

		attr := LogAttr{Key: key, AttrType: attrType}
		switch attrType {
		case LogAttrTypeStr:
			attr.StrValue = nullTermString(raw)
		case LogAttrTypeBoolean:
			attr.BoolValue = raw[0] != 0
		case LogAttrTypeInt8:
			attr.IntValue = int64(int8(raw[0]))
		case LogAttrTypeInt16:
			attr.IntValue = int64(int16(binary.LittleEndian.Uint16(raw)))
		case LogAttrTypeInt32:
			attr.IntValue = int64(int32(binary.LittleEndian.Uint32(raw)))
		case LogAttrTypeInt64:
			attr.IntValue = int64(binary.LittleEndian.Uint64(raw))
		case LogAttrTypeUInt8:
			attr.IntValue = int64(raw[0])
		case LogAttrTypeUInt16:
			attr.IntValue = int64(binary.LittleEndian.Uint16(raw))
		case LogAttrTypeUInt32:
			attr.IntValue = int64(binary.LittleEndian.Uint32(raw))
		case LogAttrTypeUInt64:
			attr.IntValue = int64(binary.LittleEndian.Uint64(raw)) // may wrap for values > MaxInt64
		case LogAttrTypeFloat32:
			attr.DoubleValue = float64(math.Float32frombits(binary.LittleEndian.Uint32(raw)))
		case LogAttrTypeFloat64:
			attr.DoubleValue = math.Float64frombits(binary.LittleEndian.Uint64(raw))
		default:
			attr.StrValue = nullTermString(raw)
		}
		attrs = append(attrs, attr)
	}

	return LogSlot{
		TimestampNs: binary.LittleEndian.Uint64(b[0:]),
		Severity:    binary.LittleEndian.Uint32(b[8:]),
		AttrCount:   attrCount,
		Message:     nullTermString(b[16:208]),
		Source:      nullTermString(b[208:272]),
		Attrs:       attrs,
		// sequence is at b[1276:1280] — validated by DrainLogs.
	}, nil
}

// DrainLogs parses new log slots from a raw log ring buffer snapshot.
func DrainLogs(rawRing []byte, localRI uint32, newWI uint32) ([]LogSlot, uint32) {
	if newWI == localRI {
		return nil, localRI
	}

	header, err := ParseMetricRingHeader(rawRing) // header layout is identical
	if err != nil {
		return nil, localRI
	}

	capacity := header.Capacity
	if capacity == 0 {
		capacity = LogCapacity
	}

	pendingCount := newWI - localRI
	if pendingCount > capacity {
		localRI += pendingCount - capacity/2
		pendingCount = capacity / 2
	}

	slots := make([]LogSlot, 0, pendingCount)

	for i := uint32(0); i < pendingCount; i++ {
		absIdx := localRI + i
		slotIdx := absIdx % capacity

		slotOff := LogHeaderSize + int(slotIdx)*LogSlotSize
		if slotOff+LogSlotSize > len(rawRing) {
			break
		}
		raw := rawRing[slotOff : slotOff+LogSlotSize]

		seq := binary.LittleEndian.Uint32(raw[1276:])
		expectedGen := ((absIdx / capacity) + 1) * 2

		if seq == expectedGen {
			slot, err := ParseLogSlot(raw)
			if err == nil {
				slots = append(slots, slot)
			}
		}
		// Torn reads and overflows are silently dropped for logs —
		// log loss is preferable to corrupted messages.
	}

	return slots, newWI
}

// nullTermString converts a fixed-length byte slice to a Go string, stopping
// at the first null byte. TwinCAT STRING(n) is 8-bit (Latin-1 / ASCII).
func nullTermString(b []byte) string {
	for i, c := range b {
		if c == 0 {
			return string(b[:i])
		}
	}
	return string(b)
}
