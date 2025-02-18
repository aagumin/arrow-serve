package iskander

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func arrayOf(mem memory.Allocator, a interface{}, valids []bool) arrow.Array {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	switch a := a.(type) {
	// case []nullT:
	//	return array.NewNull(len(a))

	case []bool:
		bldr := array.NewBooleanBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewBooleanArray()

	case []int8:
		bldr := array.NewInt8Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewInt8Array()

	case []int16:
		bldr := array.NewInt16Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewInt16Array()

	case []int32:
		bldr := array.NewInt32Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewInt32Array()

	case []int64:
		bldr := array.NewInt64Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewInt64Array()

	case []uint8:
		bldr := array.NewUint8Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewUint8Array()

	case []uint16:
		bldr := array.NewUint16Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewUint16Array()

	case []uint32:
		bldr := array.NewUint32Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewUint32Array()

	case []uint64:
		bldr := array.NewUint64Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewUint64Array()

	case []float16.Num:
		bldr := array.NewFloat16Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewFloat16Array()

	case []float32:
		bldr := array.NewFloat32Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewFloat32Array()

	case []float64:
		bldr := array.NewFloat64Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewFloat64Array()

	case []decimal128.Num:
		bldr := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 72, Scale: 2})
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		aa := bldr.NewDecimal128Array()
		return aa

	case []decimal256.Num:
		bldr := array.NewDecimal256Builder(mem, &arrow.Decimal256Type{Precision: 72, Scale: 2})
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		aa := bldr.NewDecimal256Array()
		return aa

	case []string:
		bldr := array.NewStringBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewStringArray()

	case [][]byte:
		bldr := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewBinaryArray()

	//case []time32s:
	//	bldr := array.NewTime32Builder(mem, arrow.FixedWidthTypes.Time32s.(*arrow.Time32Type))
	//	defer bldr.Release()
	//
	//	vs := make([]arrow.Time32, len(a))
	//	for i, v := range a {
	//		vs[i] = arrow.Time32(v)
	//	}
	//	bldr.AppendValues(vs, valids)
	//	return bldr.NewArray()
	//
	//case []time32ms:
	//	bldr := array.NewTime32Builder(mem, arrow.FixedWidthTypes.Time32ms.(*arrow.Time32Type))
	//	defer bldr.Release()
	//
	//	vs := make([]arrow.Time32, len(a))
	//	for i, v := range a {
	//		vs[i] = arrow.Time32(v)
	//	}
	//	bldr.AppendValues(vs, valids)
	//	return bldr.NewArray()
	//
	//case []time64ns:
	//	bldr := array.NewTime64Builder(mem, arrow.FixedWidthTypes.Time64ns.(*arrow.Time64Type))
	//	defer bldr.Release()
	//
	//	vs := make([]arrow.Time64, len(a))
	//	for i, v := range a {
	//		vs[i] = arrow.Time64(v)
	//	}
	//	bldr.AppendValues(vs, valids)
	//	return bldr.NewArray()
	//
	//case []time64us:
	//	bldr := array.NewTime64Builder(mem, arrow.FixedWidthTypes.Time64us.(*arrow.Time64Type))
	//	defer bldr.Release()
	//
	//	vs := make([]arrow.Time64, len(a))
	//	for i, v := range a {
	//		vs[i] = arrow.Time64(v)
	//	}
	//	bldr.AppendValues(vs, valids)
	//	return bldr.NewArray()
	//
	//case []timestamp_s:
	//	bldr := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_s.(*arrow.TimestampType))
	//	defer bldr.Release()
	//
	//	vs := make([]arrow.Timestamp, len(a))
	//	for i, v := range a {
	//		vs[i] = arrow.Timestamp(v)
	//	}
	//	bldr.AppendValues(vs, valids)
	//	return bldr.NewArray()
	//
	//case []timestamp_ms:
	//	bldr := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_ms.(*arrow.TimestampType))
	//	defer bldr.Release()
	//
	//	vs := make([]arrow.Timestamp, len(a))
	//	for i, v := range a {
	//		vs[i] = arrow.Timestamp(v)
	//	}
	//	bldr.AppendValues(vs, valids)
	//	return bldr.NewArray()

	//case []timestamp_us:
	//	bldr := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType))
	//	defer bldr.Release()
	//
	//	vs := make([]arrow.Timestamp, len(a))
	//	for i, v := range a {
	//		vs[i] = arrow.Timestamp(v)
	//	}
	//	bldr.AppendValues(vs, valids)
	//	return bldr.NewArray()
	//
	//case []timestamp_ns:
	//	bldr := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_ns.(*arrow.TimestampType))
	//	defer bldr.Release()
	//
	//	vs := make([]arrow.Timestamp, len(a))
	//	for i, v := range a {
	//		vs[i] = arrow.Timestamp(v)
	//	}
	//	bldr.AppendValues(vs, valids)
	//	return bldr.NewArray()

	case []arrow.Date32:
		bldr := array.NewDate32Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewArray()

	case []arrow.Date64:
		bldr := array.NewDate64Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewArray()

	// case []fsb3:
	//	bldr := array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 3})
	//	defer bldr.Release()
	//	vs := make([][]byte, len(a))
	//	for i, v := range a {
	//		vs[i] = []byte(v)
	//	}
	//	bldr.AppendValues(vs, valids)
	//	return bldr.NewArray()

	case []arrow.MonthInterval:
		bldr := array.NewMonthIntervalBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewArray()

	case []arrow.DayTimeInterval:
		bldr := array.NewDayTimeIntervalBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewArray()

	case []arrow.MonthDayNanoInterval:
		bldr := array.NewMonthDayNanoIntervalBuilder(mem)
		defer bldr.Release()

		bldr.AppendValues(a, valids)
		return bldr.NewArray()

	//case []duration_s:
	//	bldr := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Second})
	//	defer bldr.Release()
	//	vs := make([]arrow.Duration, len(a))
	//	for i, v := range a {
	//		vs[i] = arrow.Duration(v)
	//	}
	//	bldr.AppendValues(vs, valids)
	//	return bldr.NewArray()
	//
	//case []duration_ms:
	//	bldr := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Millisecond})
	//	defer bldr.Release()
	//	vs := make([]arrow.Duration, len(a))
	//	for i, v := range a {
	//		vs[i] = arrow.Duration(v)
	//	}
	//	bldr.AppendValues(vs, valids)
	//	return bldr.NewArray()
	//
	//case []duration_us:
	//	bldr := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Microsecond})
	//	defer bldr.Release()
	//	vs := make([]arrow.Duration, len(a))
	//	for i, v := range a {
	//		vs[i] = arrow.Duration(v)
	//	}
	//	bldr.AppendValues(vs, valids)
	//	return bldr.NewArray()
	//
	//case []duration_ns:
	//	bldr := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Nanosecond})
	//	defer bldr.Release()
	//	vs := make([]arrow.Duration, len(a))
	//	for i, v := range a {
	//		vs[i] = arrow.Duration(v)
	//	}
	//	bldr.AppendValues(vs, valids)
	//	return bldr.NewArray()

	default:
		panic(fmt.Errorf("arrdata: invalid data slice type %T", a))
	}
}

func makePrimitiveRecords() []arrow.Record {
	mem := memory.NewGoAllocator()

	meta := arrow.NewMetadata(
		[]string{"k1", "k2", "k3"},
		[]string{"v1", "v2", "v3"},
	)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "bools", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			{Name: "int8s", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
			{Name: "int16s", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
			{Name: "int32s", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "int64s", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "uint8s", Type: arrow.PrimitiveTypes.Uint8, Nullable: true},
			{Name: "uint16s", Type: arrow.PrimitiveTypes.Uint16, Nullable: true},
			{Name: "uint32s", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
			{Name: "uint64s", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
			{Name: "float32s", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
			{Name: "float64s", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		}, &meta,
	)

	mask := []bool{true, false, false, true, true}
	chunks := [][]arrow.Array{
		{
			arrayOf(mem, []bool{true, false, true, false, true}, mask),
			arrayOf(mem, []int8{-1, -2, -3, -4, -5}, mask),
			arrayOf(mem, []int16{-1, -2, -3, -4, -5}, mask),
			arrayOf(mem, []int32{-1, -2, -3, -4, -5}, mask),
			arrayOf(mem, []int64{-1, -2, -3, -4, -5}, mask),
			arrayOf(mem, []uint8{+1, +2, +3, +4, +5}, mask),
			arrayOf(mem, []uint16{+1, +2, +3, +4, +5}, mask),
			arrayOf(mem, []uint32{+1, +2, +3, +4, +5}, mask),
			arrayOf(mem, []uint64{+1, +2, +3, +4, +5}, mask),
			arrayOf(mem, []float32{+1, +2, +3, +4, +5}, mask),
			arrayOf(mem, []float64{+1, +2, +3, +4, +5}, mask),
		},
		{
			arrayOf(mem, []bool{true, false, true, false, true}, mask),
			arrayOf(mem, []int8{-11, -12, -13, -14, -15}, mask),
			arrayOf(mem, []int16{-11, -12, -13, -14, -15}, mask),
			arrayOf(mem, []int32{-11, -12, -13, -14, -15}, mask),
			arrayOf(mem, []int64{-11, -12, -13, -14, -15}, mask),
			arrayOf(mem, []uint8{+11, +12, +13, +14, +15}, mask),
			arrayOf(mem, []uint16{+11, +12, +13, +14, +15}, mask),
			arrayOf(mem, []uint32{+11, +12, +13, +14, +15}, mask),
			arrayOf(mem, []uint64{+11, +12, +13, +14, +15}, mask),
			arrayOf(mem, []float32{+11, +12, +13, +14, +15}, mask),
			arrayOf(mem, []float64{+11, +12, +13, +14, +15}, mask),
		},
		{
			arrayOf(mem, []bool{true, false, true, false, true}, mask),
			arrayOf(mem, []int8{-21, -22, -23, -24, -25}, mask),
			arrayOf(mem, []int16{-21, -22, -23, -24, -25}, mask),
			arrayOf(mem, []int32{-21, -22, -23, -24, -25}, mask),
			arrayOf(mem, []int64{-21, -22, -23, -24, -25}, mask),
			arrayOf(mem, []uint8{+21, +22, +23, +24, +25}, mask),
			arrayOf(mem, []uint16{+21, +22, +23, +24, +25}, mask),
			arrayOf(mem, []uint32{+21, +22, +23, +24, +25}, mask),
			arrayOf(mem, []uint64{+21, +22, +23, +24, +25}, mask),
			arrayOf(mem, []float32{+21, +22, +23, +24, +25}, mask),
			arrayOf(mem, []float64{+21, +22, +23, +24, +25}, mask),
		},
	}

	defer func() {
		for _, chunk := range chunks {
			for _, col := range chunk {
				col.Release()
			}
		}
	}()

	recs := make([]arrow.Record, len(chunks))
	for i, chunk := range chunks {
		recs[i] = array.NewRecord(schema, chunk, -1)
	}

	return recs
}
