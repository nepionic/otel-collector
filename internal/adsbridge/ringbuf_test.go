package adsbridge

import (
	"encoding/binary"
	"math"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

// putNullTerm copies s into dst as a null-terminated string.
// dst is pre-zeroed so bytes after len(s) are already 0.
func putNullTerm(dst []byte, s string) {
	copy(dst, s)
}

// metricSlotBytes builds a fully-populated 672-byte metric slot.
// seq is written into the sequence field at offset 668.
func metricSlotBytes(seq uint32, tsNs uint64, kind uint32, name, unit string, value float64, attrs []MetricAttr) []byte {
	b := make([]byte, MetricSlotSize)
	binary.LittleEndian.PutUint64(b[0:], tsNs)
	binary.LittleEndian.PutUint32(b[8:], kind)
	binary.LittleEndian.PutUint32(b[12:], uint32(len(attrs)))
	putNullTerm(b[16:144], name)
	putNullTerm(b[144:176], unit)
	binary.LittleEndian.PutUint64(b[176:], math.Float64bits(value))
	for i, a := range attrs {
		if i >= MetricAttrCount {
			break
		}
		base := 184 + i*(MetricAttrKeySize+MetricAttrValSize)
		putNullTerm(b[base:base+MetricAttrKeySize], a.Key)
		putNullTerm(b[base+MetricAttrKeySize:base+MetricAttrKeySize+MetricAttrValSize], a.Value)
	}
	binary.LittleEndian.PutUint32(b[668:], seq)
	return b
}

// makeMetricRing builds a raw ring buffer byte slice with the given header
// fields and slots. slots maps absolute write-index → pre-built 672-byte slot;
// the builder places each slot at slotIdx = absIdx % capacity.
func makeMetricRing(capacity, head, overflowCnt uint32, slots map[uint32][]byte) []byte {
	total := MetricHeaderSize + int(capacity)*MetricSlotSize
	b := make([]byte, total)
	binary.LittleEndian.PutUint32(b[0:], head)
	binary.LittleEndian.PutUint32(b[4:], capacity)
	binary.LittleEndian.PutUint32(b[8:], overflowCnt)
	for absIdx, slot := range slots {
		slotIdx := absIdx % capacity
		off := MetricHeaderSize + int(slotIdx)*MetricSlotSize
		copy(b[off:], slot)
	}
	return b
}

// seqFor returns the committed (even) sequence number expected by DrainMetrics
// for a slot at the given absolute index in a ring of the given capacity.
// Mirrors the PLC formula: expectedGen = (absIdx / capacity + 1) * 2
func seqFor(absIdx, capacity uint32) uint32 {
	return (absIdx/capacity + 1) * 2
}

// committedSlot is a convenience builder: creates a slot at absIdx in a ring
// of testCapacity with the correct committed sequence, a fixed timestamp, and
// a gauge kind.
const testCapacity = uint32(4)

func committedSlot(absIdx uint32, name string, value float64) []byte {
	return metricSlotBytes(seqFor(absIdx, testCapacity), 1_000_000_000, MetricKindGauge, name, "u", value, nil)
}

// ─────────────────────────────────────────────────────────────────────────────
// ParseMetricRingHeader
// ─────────────────────────────────────────────────────────────────────────────

func TestParseMetricRingHeader_TooShort(t *testing.T) {
	_, err := ParseMetricRingHeader(make([]byte, MetricHeaderSize-1))
	if err == nil {
		t.Fatal("expected error for buffer shorter than MetricHeaderSize")
	}
}

func TestParseMetricRingHeader_Valid(t *testing.T) {
	b := make([]byte, MetricHeaderSize)
	binary.LittleEndian.PutUint32(b[0:], 42)  // WriteIndex
	binary.LittleEndian.PutUint32(b[4:], 256) // Capacity
	binary.LittleEndian.PutUint32(b[8:], 3)   // OverflowCnt

	h, err := ParseMetricRingHeader(b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.WriteIndex != 42 {
		t.Errorf("WriteIndex: want 42, got %d", h.WriteIndex)
	}
	if h.Capacity != 256 {
		t.Errorf("Capacity: want 256, got %d", h.Capacity)
	}
	if h.OverflowCnt != 3 {
		t.Errorf("OverflowCnt: want 3, got %d", h.OverflowCnt)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ParseMetricSlot
// ─────────────────────────────────────────────────────────────────────────────

func TestParseMetricSlot_TooShort(t *testing.T) {
	_, err := ParseMetricSlot(make([]byte, MetricSlotSize-1))
	if err == nil {
		t.Fatal("expected error for buffer shorter than MetricSlotSize")
	}
}

func TestParseMetricSlot_Basic(t *testing.T) {
	const tsNs = uint64(1_700_000_000_123_456_789)
	b := metricSlotBytes(2, tsNs, MetricKindGauge, "plc.temperature", "Cel", 98.6, nil)

	slot, err := ParseMetricSlot(b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slot.TimestampNs != tsNs {
		t.Errorf("TimestampNs: want %d, got %d", tsNs, slot.TimestampNs)
	}
	if slot.Kind != MetricKindGauge {
		t.Errorf("Kind: want %d (Gauge), got %d", MetricKindGauge, slot.Kind)
	}
	if slot.Name != "plc.temperature" {
		t.Errorf("Name: want %q, got %q", "plc.temperature", slot.Name)
	}
	if slot.Unit != "Cel" {
		t.Errorf("Unit: want %q, got %q", "Cel", slot.Unit)
	}
	if math.Abs(slot.ValueF64-98.6) > 1e-9 {
		t.Errorf("ValueF64: want ~98.6, got %v", slot.ValueF64)
	}
	if len(slot.Attrs) != 0 {
		t.Errorf("Attrs: want 0, got %d", len(slot.Attrs))
	}
}

func TestParseMetricSlot_Attrs(t *testing.T) {
	attrs := []MetricAttr{
		{Key: "axis.id", Value: "3"},
		{Key: "station", Value: "feed"},
	}
	b := metricSlotBytes(2, 0, MetricKindCounter, "plc.cycles", "{cycles}", 1.0, attrs)

	slot, err := ParseMetricSlot(b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(slot.Attrs) != 2 {
		t.Fatalf("Attrs len: want 2, got %d", len(slot.Attrs))
	}
	if slot.Attrs[0].Key != "axis.id" || slot.Attrs[0].Value != "3" {
		t.Errorf("Attrs[0]: want {axis.id 3}, got %+v", slot.Attrs[0])
	}
	if slot.Attrs[1].Key != "station" || slot.Attrs[1].Value != "feed" {
		t.Errorf("Attrs[1]: want {station feed}, got %+v", slot.Attrs[1])
	}
}

func TestParseMetricSlot_AttrCountClamped(t *testing.T) {
	// attr_count > MetricAttrCount must be clamped so we don't over-read.
	b := metricSlotBytes(2, 0, MetricKindGauge, "m", "u", 0, nil)
	binary.LittleEndian.PutUint32(b[12:], 99) // absurd attr_count

	slot, err := ParseMetricSlot(b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(slot.Attrs) > MetricAttrCount {
		t.Errorf("Attrs len %d exceeds MetricAttrCount %d", len(slot.Attrs), MetricAttrCount)
	}
}

func TestParseMetricSlot_NullTerminatedName(t *testing.T) {
	// The name field is 128 bytes; bytes after the null must be ignored.
	b := metricSlotBytes(2, 0, MetricKindGauge, "abc", "u", 0, nil)
	// Poison the bytes after "abc\0" to confirm null-termination is respected.
	b[16+4] = 'X'
	b[16+5] = 'Y'

	slot, err := ParseMetricSlot(b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slot.Name != "abc" {
		t.Errorf("Name: want %q, got %q", "abc", slot.Name)
	}
}

func TestParseMetricSlot_KindCounter(t *testing.T) {
	b := metricSlotBytes(2, 0, MetricKindCounter, "events", "{events}", 5.0, nil)
	slot, err := ParseMetricSlot(b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slot.Kind != MetricKindCounter {
		t.Errorf("Kind: want %d (Counter), got %d", MetricKindCounter, slot.Kind)
	}
}

func TestParseMetricSlot_KindUpDownCounter(t *testing.T) {
	b := metricSlotBytes(2, 0, MetricKindUpDownCounter, "queue", "{items}", -2.0, nil)
	slot, err := ParseMetricSlot(b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slot.Kind != MetricKindUpDownCounter {
		t.Errorf("Kind: want %d (UpDownCounter), got %d", MetricKindUpDownCounter, slot.Kind)
	}
	if slot.ValueF64 != -2.0 {
		t.Errorf("ValueF64: want -2.0, got %v", slot.ValueF64)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// DrainMetrics
// ─────────────────────────────────────────────────────────────────────────────

func TestDrainMetrics_NoNewSlots(t *testing.T) {
	ring := makeMetricRing(testCapacity, 3, 0, nil)
	result, newRI := DrainMetrics(ring, 3, 3)

	if len(result.Slots) != 0 {
		t.Errorf("Slots: want 0, got %d", len(result.Slots))
	}
	if newRI != 3 {
		t.Errorf("newRI: want 3, got %d", newRI)
	}
	if result.TornReads != 0 || result.Overflows != 0 {
		t.Errorf("want no torn reads or overflows, got TornReads=%d Overflows=%d", result.TornReads, result.Overflows)
	}
}

func TestDrainMetrics_SingleSlot(t *testing.T) {
	slot := committedSlot(0, "plc.temp", 42.0)
	ring := makeMetricRing(testCapacity, 1, 0, map[uint32][]byte{0: slot})

	result, newRI := DrainMetrics(ring, 0, 1)

	if len(result.Slots) != 1 {
		t.Fatalf("Slots len: want 1, got %d", len(result.Slots))
	}
	if result.Slots[0].Name != "plc.temp" {
		t.Errorf("Name: want %q, got %q", "plc.temp", result.Slots[0].Name)
	}
	if math.Abs(result.Slots[0].ValueF64-42.0) > 1e-9 {
		t.Errorf("ValueF64: want 42.0, got %v", result.Slots[0].ValueF64)
	}
	if newRI != 1 {
		t.Errorf("newRI: want 1, got %d", newRI)
	}
}

func TestDrainMetrics_MultipleSlots(t *testing.T) {
	slots := map[uint32][]byte{
		0: committedSlot(0, "m0", 0),
		1: committedSlot(1, "m1", 1),
		2: committedSlot(2, "m2", 2),
	}
	ring := makeMetricRing(testCapacity, 3, 0, slots)

	result, newRI := DrainMetrics(ring, 0, 3)

	if len(result.Slots) != 3 {
		t.Fatalf("Slots len: want 3, got %d", len(result.Slots))
	}
	for i, want := range []string{"m0", "m1", "m2"} {
		if result.Slots[i].Name != want {
			t.Errorf("Slots[%d].Name: want %q, got %q", i, want, result.Slots[i].Name)
		}
	}
	if newRI != 3 {
		t.Errorf("newRI: want 3, got %d", newRI)
	}
}

func TestDrainMetrics_TornRead(t *testing.T) {
	// seq=1 is ODD — write in progress; slot should be skipped and TornReads++.
	tornSlot := metricSlotBytes(1, 0, MetricKindGauge, "torn", "u", 0, nil)
	ring := makeMetricRing(testCapacity, 1, 0, map[uint32][]byte{0: tornSlot})

	result, newRI := DrainMetrics(ring, 0, 1)

	if len(result.Slots) != 0 {
		t.Errorf("Slots: want 0, got %d", len(result.Slots))
	}
	if result.TornReads != 1 {
		t.Errorf("TornReads: want 1, got %d", result.TornReads)
	}
	if newRI != 1 {
		t.Errorf("newRI: want 1, got %d", newRI)
	}
}

func TestDrainMetrics_GenerationMismatch(t *testing.T) {
	// absIdx=4, capacity=4: expectedGen=(4/4+1)*2=4, but slot has seq=2 (gen 1 — stale).
	// Even sequence but wrong generation → Overflows++.
	staleSlot := metricSlotBytes(2, 0, MetricKindGauge, "stale", "u", 0, nil)
	ring := makeMetricRing(testCapacity, 5, 0, map[uint32][]byte{0: staleSlot})

	result, _ := DrainMetrics(ring, 4, 5)

	if len(result.Slots) != 0 {
		t.Errorf("Slots: want 0, got %d", len(result.Slots))
	}
	if result.Overflows != 1 {
		t.Errorf("Overflows: want 1, got %d", result.Overflows)
	}
}

func TestDrainMetrics_ConsumerFallenBehind(t *testing.T) {
	// localRI=0, newWI=6, capacity=4 → pendingCount=6 > 4.
	// lost = 6 - capacity/2 = 4 → fast-forward to localRI=4, pendingCount=2.
	// Reads absIdx 4 (slotIdx 0, seq=4) and absIdx 5 (slotIdx 1, seq=4).
	slots := map[uint32][]byte{
		4: metricSlotBytes(seqFor(4, testCapacity), 0, MetricKindGauge, "caught-up-0", "u", 10, nil),
		5: metricSlotBytes(seqFor(5, testCapacity), 0, MetricKindGauge, "caught-up-1", "u", 11, nil),
	}
	ring := makeMetricRing(testCapacity, 6, 0, slots)

	result, newRI := DrainMetrics(ring, 0, 6)

	if result.Overflows != 4 {
		t.Errorf("Overflows: want 4 (lost slots), got %d", result.Overflows)
	}
	if len(result.Slots) != 2 {
		t.Fatalf("Slots len: want 2, got %d", len(result.Slots))
	}
	if result.Slots[0].Name != "caught-up-0" {
		t.Errorf("Slots[0].Name: want %q, got %q", "caught-up-0", result.Slots[0].Name)
	}
	if result.Slots[1].Name != "caught-up-1" {
		t.Errorf("Slots[1].Name: want %q, got %q", "caught-up-1", result.Slots[1].Name)
	}
	if newRI != 6 {
		t.Errorf("newRI: want 6, got %d", newRI)
	}
}

func TestDrainMetrics_RingWrap(t *testing.T) {
	// localRI=3, newWI=5. Reads:
	//   absIdx=3 → slotIdx=3, expectedGen=(3/4+1)*2=2
	//   absIdx=4 → slotIdx=0, expectedGen=(4/4+1)*2=4  (slot 0 re-used)
	slots := map[uint32][]byte{
		3: metricSlotBytes(seqFor(3, testCapacity), 0, MetricKindGauge, "pre-wrap", "u", 3, nil),
		4: metricSlotBytes(seqFor(4, testCapacity), 0, MetricKindGauge, "post-wrap", "u", 4, nil),
	}
	ring := makeMetricRing(testCapacity, 5, 0, slots)

	result, newRI := DrainMetrics(ring, 3, 5)

	if len(result.Slots) != 2 {
		t.Fatalf("Slots len: want 2, got %d", len(result.Slots))
	}
	if result.Slots[0].Name != "pre-wrap" {
		t.Errorf("Slots[0].Name: want %q, got %q", "pre-wrap", result.Slots[0].Name)
	}
	if result.Slots[1].Name != "post-wrap" {
		t.Errorf("Slots[1].Name: want %q, got %q", "post-wrap", result.Slots[1].Name)
	}
	if newRI != 5 {
		t.Errorf("newRI: want 5, got %d", newRI)
	}
}

func TestDrainMetrics_OverflowCntPassedThrough(t *testing.T) {
	// overflow_cnt from the ring header should be returned in DrainResult.OverflowCnt.
	slot := committedSlot(0, "m", 0)
	ring := makeMetricRing(testCapacity, 1, 7, map[uint32][]byte{0: slot})

	result, _ := DrainMetrics(ring, 0, 1)

	if result.OverflowCnt != 7 {
		t.Errorf("OverflowCnt: want 7, got %d", result.OverflowCnt)
	}
}

func TestDrainMetrics_ZeroCapacityFallback(t *testing.T) {
	// If header.Capacity is 0, DrainMetrics must not divide by zero.
	// It falls back to a sane capacity constant (2048).
	// Use a ring big enough for the fallback capacity to avoid out-of-bounds.
	const fallback = uint32(2048)
	slot := metricSlotBytes(seqFor(0, fallback), 0, MetricKindGauge, "ok", "u", 1, nil)
	// Build ring with capacity=0 in header but actual buffer sized for fallback.
	total := MetricHeaderSize + int(fallback)*MetricSlotSize
	b := make([]byte, total)
	binary.LittleEndian.PutUint32(b[0:], 1) // head=1
	binary.LittleEndian.PutUint32(b[4:], 0) // capacity=0 → fallback
	copy(b[MetricHeaderSize:], slot)        // slot at physical index 0

	result, _ := DrainMetrics(b, 0, 1)

	if len(result.Slots) != 1 {
		t.Errorf("Slots len: want 1, got %d (zero-capacity fallback failed)", len(result.Slots))
	}
}

func TestDrainMetrics_InsufficientBufferBreaks(t *testing.T) {
	// If the raw buffer is shorter than a full slot, DrainMetrics should break
	// without panicking, returning fewer slots than newWI - localRI.
	// Build a header-only buffer (no slot data).
	ring := makeMetricRing(testCapacity, 2, 0, nil)
	truncated := ring[:MetricHeaderSize] // only header, no slot bytes

	result, _ := DrainMetrics(truncated, 0, 2)

	// Should return 0 slots (broke on first slot offset check) without panicking.
	if len(result.Slots) != 0 {
		t.Errorf("Slots len: want 0 for truncated buffer, got %d", len(result.Slots))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TimestampNsToTime
// ─────────────────────────────────────────────────────────────────────────────

func TestTimestampNsToTime_Zero(t *testing.T) {
	before := time.Now()
	got := TimestampNsToTime(0)
	after := time.Now()

	if got.Before(before) || got.After(after) {
		t.Errorf("zero timestamp: want approx time.Now(), got %v (window [%v, %v])", got, before, after)
	}
}

func TestTimestampNsToTime_NonZero(t *testing.T) {
	const ns = uint64(1_700_000_000_123_456_789)
	got := TimestampNsToTime(ns)
	if got.UnixNano() != int64(ns) {
		t.Errorf("UnixNano: want %d, got %d", int64(ns), got.UnixNano())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ParseLogSlot — smoke tests (deeper coverage lives with log-specific tests)
// ─────────────────────────────────────────────────────────────────────────────

func TestParseLogSlot_TooShort(t *testing.T) {
	_, err := ParseLogSlot(make([]byte, LogSlotSize-1))
	if err == nil {
		t.Fatal("expected error for buffer shorter than LogSlotSize")
	}
}

func TestParseLogSlot_BasicMessage(t *testing.T) {
	b := make([]byte, LogSlotSize)
	const tsNs = uint64(1_700_000_000_000_000_000)
	binary.LittleEndian.PutUint64(b[0:], tsNs) // timestamp
	binary.LittleEndian.PutUint32(b[8:], 1)    // severity = Info
	binary.LittleEndian.PutUint32(b[12:], 0)   // attr_count = 0
	putNullTerm(b[16:208], "hello from PLC")   // message
	putNullTerm(b[208:272], "MAIN.FB_Logger")  // source
	binary.LittleEndian.PutUint32(b[1276:], 2) // sequence = committed

	slot, err := ParseLogSlot(b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slot.TimestampNs != tsNs {
		t.Errorf("TimestampNs: want %d, got %d", tsNs, slot.TimestampNs)
	}
	if slot.Severity != 1 {
		t.Errorf("Severity: want 1, got %d", slot.Severity)
	}
	if slot.Message != "hello from PLC" {
		t.Errorf("Message: want %q, got %q", "hello from PLC", slot.Message)
	}
	if slot.Source != "MAIN.FB_Logger" {
		t.Errorf("Source: want %q, got %q", "MAIN.FB_Logger", slot.Source)
	}
}

func TestParseLogSlot_StringAttr(t *testing.T) {
	b := make([]byte, LogSlotSize)
	binary.LittleEndian.PutUint32(b[12:], 1) // attr_count = 1
	putNullTerm(b[16:208], "msg")

	// Build attr at offset 272: key(32) + attr_type(1) + value_bytes(63)
	putNullTerm(b[272:272+LogKeySize], "batch.id")
	b[272+LogAttrTypeOff] = uint8(LogAttrTypeStr)
	putNullTerm(b[272+LogAttrValOff:272+LogAttrValOff+LogAttrValLen], "BATCH-42")
	binary.LittleEndian.PutUint32(b[1276:], 2) // committed

	slot, err := ParseLogSlot(b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(slot.Attrs) != 1 {
		t.Fatalf("Attrs len: want 1, got %d", len(slot.Attrs))
	}
	if slot.Attrs[0].Key != "batch.id" {
		t.Errorf("Attr[0].Key: want %q, got %q", "batch.id", slot.Attrs[0].Key)
	}
	if slot.Attrs[0].StrValue != "BATCH-42" {
		t.Errorf("Attr[0].StrValue: want %q, got %q", "BATCH-42", slot.Attrs[0].StrValue)
	}
}

func TestParseLogSlot_NumericAttrs(t *testing.T) {
	b := make([]byte, LogSlotSize)
	binary.LittleEndian.PutUint32(b[12:], 3) // attr_count = 3
	putNullTerm(b[16:208], "msg")

	// Attr 0: UDINT (LogAttrTypeUInt32) = 999
	attrBase := func(i int) int { return 272 + i*LogAttrSize }
	putNullTerm(b[attrBase(0):attrBase(0)+LogKeySize], "error.code")
	b[attrBase(0)+LogAttrTypeOff] = uint8(LogAttrTypeUInt32)
	binary.LittleEndian.PutUint32(b[attrBase(0)+LogAttrValOff:], 999)

	// Attr 1: LREAL (LogAttrTypeFloat64) = 3.14
	putNullTerm(b[attrBase(1):attrBase(1)+LogKeySize], "position")
	b[attrBase(1)+LogAttrTypeOff] = uint8(LogAttrTypeFloat64)
	binary.LittleEndian.PutUint64(b[attrBase(1)+LogAttrValOff:], math.Float64bits(3.14))

	// Attr 2: BOOL (LogAttrTypeBoolean) = true
	putNullTerm(b[attrBase(2):attrBase(2)+LogKeySize], "ready")
	b[attrBase(2)+LogAttrTypeOff] = uint8(LogAttrTypeBoolean)
	b[attrBase(2)+LogAttrValOff] = 1

	binary.LittleEndian.PutUint32(b[1276:], 2)

	slot, err := ParseLogSlot(b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slot.Attrs[0].IntValue != 999 {
		t.Errorf("Attr[0].IntValue: want 999, got %d", slot.Attrs[0].IntValue)
	}
	if math.Abs(slot.Attrs[1].DoubleValue-3.14) > 1e-9 {
		t.Errorf("Attr[1].DoubleValue: want 3.14, got %v", slot.Attrs[1].DoubleValue)
	}
	if !slot.Attrs[2].BoolValue {
		t.Errorf("Attr[2].BoolValue: want true, got false")
	}
}
