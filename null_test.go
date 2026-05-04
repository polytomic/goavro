// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import "testing"

func TestSchemaPrimitiveNullCodec(t *testing.T) {
	testSchemaPrimativeCodec(t, `"null"`)
}

func TestPrimitiveNullBinary(t *testing.T) {
	// The null codec returns a package-level sentinel rather than a
	// formatted error; the union encoder constructs its own diagnostic
	// (with the received type) when no member codec accepts the datum.
	testBinaryEncodeFail(t, `"null"`, false, "expected: Go nil")
	testBinaryCodecPass(t, `"null"`, nil, nil)
}

func TestPrimitiveNullText(t *testing.T) {
	testTextEncodeFail(t, `"null"`, false, "expected: Go nil")
	testTextCodecPass(t, `"null"`, nil, []byte("null"))
}
