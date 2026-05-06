// Copyright [2026] Polytomic, Inc. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package goavro

import (
	"strconv"
	"testing"
)

// BenchmarkNullableUnionRecord measures the cost of encoding records whose
// fields are predominantly ["null", T] unions. This is the workload that
// dominates Polytomic's destination writers (Databricks, S3, blobstore): a
// schema with dozens of nullable scalar fields, encoded once per record over
// millions of records.
//
// Each benchmark variant runs both with the fast path enabled and disabled so
// the speedup attributable to the fast path is visible directly. Compare like
// against like: e.g. BenchmarkNullableUnionRecord/wide_mixed/fast vs
// BenchmarkNullableUnionRecord/wide_mixed/slow.

// recordSchema with `numFields` nullable fields, cycling through string,
// long, double, and boolean types. Field names are field0, field1, ...
func nullableRecordSchema(numFields int) string {
	types := []string{`"string"`, `"long"`, `"double"`, `"boolean"`}
	var b []byte
	b = append(b, `{"type":"record","name":"r","fields":[`...)
	for i := 0; i < numFields; i++ {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, `{"name":"f`...)
		b = strconv.AppendInt(b, int64(i), 10)
		b = append(b, `","type":["null",`...)
		b = append(b, types[i%len(types)]...)
		b = append(b, `]}`...)
	}
	b = append(b, `]}`...)
	return string(b)
}

// nullableRecordValue returns a record where `nilFraction` of the fields are
// nil (in [0.0, 1.0]). Non-nil fields get a typed value matching their schema.
// Field 0 is field0, etc., matching nullableRecordSchema.
func nullableRecordValue(numFields int, nilFraction float64) map[string]interface{} {
	rec := make(map[string]interface{}, numFields)
	threshold := int(nilFraction * float64(numFields))
	for i := 0; i < numFields; i++ {
		name := "f" + strconv.Itoa(i)
		if i < threshold {
			rec[name] = nil
			continue
		}
		switch i % 4 {
		case 0:
			rec[name] = "value-" + strconv.Itoa(i)
		case 1:
			rec[name] = int64(i)
		case 2:
			rec[name] = float64(i) * 1.5
		case 3:
			rec[name] = (i%2 == 0)
		}
	}
	return rec
}

func benchmarkNullableUnionRecord(b *testing.B, numFields int, nilFraction float64, disableFast bool) {
	codec, err := NewCodec(nullableRecordSchema(numFields))
	if err != nil {
		b.Fatal(err)
	}
	rec := nullableRecordValue(numFields, nilFraction)

	// Warm a buffer of plausible size to keep allocation noise out of the
	// per-op accounting; the encoder appends, so each iteration starts
	// fresh from a zero-length slice but with capacity reserved.
	buf := make([]byte, 0, 256)

	prev := nullableFastPathDisabled
	nullableFastPathDisabled = disableFast
	defer func() { nullableFastPathDisabled = prev }()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := codec.BinaryFromNative(buf[:0], rec)
		if err != nil {
			b.Fatal(err)
		}
		buf = out
	}
}

func BenchmarkNullableUnionRecord(b *testing.B) {
	// Field counts and nil fractions chosen to bracket the destination
	// writer workload: schemas typically have 20–80 nullable fields, with
	// nil fractions ranging from very dense (~10%) for normalized event
	// tables to sparse (~70%) for wide CRM-style records.
	configs := []struct {
		name        string
		numFields   int
		nilFraction float64
	}{
		{"narrow_dense", 8, 0.1},  // 8 fields, 10% null
		{"narrow_sparse", 8, 0.7}, // 8 fields, 70% null
		{"wide_dense", 40, 0.1},   // 40 fields, 10% null
		{"wide_mixed", 40, 0.5},   // 40 fields, 50% null
		{"wide_sparse", 40, 0.7},  // 40 fields, 70% null
		{"jumbo_mixed", 100, 0.5}, // 100 fields, 50% null
	}
	for _, cfg := range configs {
		cfg := cfg
		b.Run(cfg.name+"/fast", func(b *testing.B) {
			benchmarkNullableUnionRecord(b, cfg.numFields, cfg.nilFraction, false)
		})
		b.Run(cfg.name+"/slow", func(b *testing.B) {
			benchmarkNullableUnionRecord(b, cfg.numFields, cfg.nilFraction, true)
		})
	}
}

// BenchmarkNullableUnionScalar isolates the union-encode cost from the
// surrounding record-encode work. Useful for attributing time to the fast path
// itself rather than to record framing.
func BenchmarkNullableUnionScalar(b *testing.B) {
	cases := []struct {
		name   string
		schema string
		datum  interface{}
	}{
		{"string_value", `["null","string"]`, "hello world"},
		{"string_nil", `["null","string"]`, nil},
		{"long_value", `["null","long"]`, int64(1234567890)},
		{"long_nil", `["null","long"]`, nil},
		{"double_value", `["null","double"]`, 3.14159},
		{"boolean_value", `["null","boolean"]`, true},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name+"/fast", func(b *testing.B) {
			benchmarkScalarUnion(b, tc.schema, tc.datum, false)
		})
		b.Run(tc.name+"/slow", func(b *testing.B) {
			benchmarkScalarUnion(b, tc.schema, tc.datum, true)
		})
	}
}

func benchmarkScalarUnion(b *testing.B, schema string, datum interface{}, disableFast bool) {
	codec, err := NewCodec(schema)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 0, 64)

	prev := nullableFastPathDisabled
	nullableFastPathDisabled = disableFast
	defer func() { nullableFastPathDisabled = prev }()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := codec.BinaryFromNative(buf[:0], datum)
		if err != nil {
			b.Fatal(err)
		}
		buf = out
	}
}
