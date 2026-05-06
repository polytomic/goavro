// Copyright [2026] Polytomic, Inc. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package goavro

import (
	"bytes"
	"fmt"
	"testing"
)

// Tests in this file pin down the byte-for-byte behavior of the nullable-union
// fast path against the slow path. They exist independently of the existing
// TestUnion suite because that suite has pre-existing failures on master that
// would otherwise drown out regressions specific to the fast path.

func TestNullableUnionFastPathByteIdentity(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		datum  interface{}
	}{
		{`null/string nil`, `["null","string"]`, nil},
		{`null/string value`, `["null","string"]`, "hello"},
		{`null/string empty`, `["null","string"]`, ""},
		{`string/null nil`, `["string","null"]`, nil},
		{`string/null value`, `["string","null"]`, "hello"},
		{`null/int nil`, `["null","int"]`, nil},
		{`null/int value`, `["null","int"]`, 42},
		{`null/int int32`, `["null","int"]`, int32(42)},
		{`null/long`, `["null","long"]`, int64(123456789)},
		{`null/double`, `["null","double"]`, 3.14},
		{`null/boolean true`, `["null","boolean"]`, true},
		{`null/boolean false`, `["null","boolean"]`, false},
		{`null/bytes`, `["null","bytes"]`, []byte("hello")},
		{`null/array`, `["null",{"type":"array","items":"int"}]`, []interface{}{1, 2, 3}},
		{`null/map`, `["null",{"type":"map","values":"string"}]`, map[string]interface{}{"k": "v"}},
		// Explicit Union wrapping must continue to take the existing
		// branch and produce identical bytes to the bare-value fast path.
		{`Union wrap string`, `["null","string"]`, Union("string", "hello")},
		{`Union wrap int`, `["null","int"]`, Union("int", 42)},
		{`Union null`, `["null","string"]`, Union("null", nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fast, err := encodeOnce(tc.schema, tc.datum, false /* fast on */)
			if err != nil {
				t.Fatalf("fast path encode: %v", err)
			}
			slow, err := encodeOnce(tc.schema, tc.datum, true /* fast off */)
			if err != nil {
				t.Fatalf("slow path encode: %v", err)
			}
			if !bytes.Equal(fast, slow) {
				t.Errorf("byte mismatch:\n fast: % x\n slow: % x", fast, slow)
			}
		})
	}
}

func TestNullableUnionFastPathRoundTrip(t *testing.T) {
	cases := []struct {
		schema string
		datum  interface{}
		want   string
	}{
		{`["null","string"]`, "hello", `map[string:hello]`},
		{`["null","int"]`, 42, `map[int:42]`},
		{`["null","long"]`, int64(42), `map[long:42]`},
		{`["null","string"]`, nil, `<nil>`},
	}
	for _, tc := range cases {
		t.Run(tc.schema, func(t *testing.T) {
			codec, err := NewCodec(tc.schema)
			if err != nil {
				t.Fatal(err)
			}
			buf, err := codec.BinaryFromNative(nil, tc.datum)
			if err != nil {
				t.Fatal(err)
			}
			got, _, err := codec.NativeFromBinary(buf)
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprintf("%v", got) != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNullableUnionFastPathRecursiveSchema(t *testing.T) {
	// Recursive schemas register their codec with nil function fields and
	// patch them in later. The fast path stores the *Codec, not its
	// binaryFromNative func, so the resolved encoder is visible at call
	// time. This test would panic with a nil-deref if we cached the func.
	codec, err := NewCodec(`{
        "type": "record",
        "name": "LongList",
        "fields": [
            {"name": "next", "type": ["null", "LongList"], "default": null}
        ]
    }`)
	if err != nil {
		t.Fatal(err)
	}
	datum := map[string]interface{}{
		"next": Union("LongList", map[string]interface{}{
			"next": Union("LongList", map[string]interface{}{
				"next": nil,
			}),
		}),
	}
	buf, err := codec.BinaryFromNative(nil, datum)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// next=LongList → 0x02; inner next=LongList → 0x02; innermost next=null → 0x00
	want := []byte{0x02, 0x02, 0x00}
	if !bytes.Equal(buf, want) {
		t.Errorf("got % x, want % x", buf, want)
	}
}

func TestNullableUnionFastPathErrorMessage(t *testing.T) {
	fast, err := encodeOnce(`["null","int"]`, "not an int", false /* fast on */)
	if err == nil {
		t.Fatalf("fast path unexpectedly succeeded: % x", fast)
	}
	slow, slowErr := encodeOnce(`["null","int"]`, "not an int", true /* fast off */)
	if slowErr == nil {
		t.Fatalf("slow path unexpectedly succeeded: % x", slow)
	}
	if err.Error() != slowErr.Error() {
		t.Fatalf("error mismatch:\n fast: %v\n slow: %v", err, slowErr)
	}
}

// encodeOnce builds a fresh codec, optionally disables the nullable-union
// fast path for the duration of the call, and encodes datum.
func encodeOnce(schema string, datum interface{}, disableFast bool) ([]byte, error) {
	codec, err := NewCodec(schema)
	if err != nil {
		return nil, err
	}
	prev := nullableFastPathDisabled
	nullableFastPathDisabled = disableFast
	defer func() { nullableFastPathDisabled = prev }()
	return codec.BinaryFromNative(nil, datum)
}
