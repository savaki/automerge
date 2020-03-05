// Copyright 2020 Matt Ho
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package encoding

import (
	"testing"
	"unicode/utf8"

	"github.com/tj/assert"
)

func TestEquals(t *testing.T) {
	assert.EqualValues(t, 4, lengthUint32)
}

func TestEncodeVarInt(t *testing.T) {
	assert.Equal(t, 3, utf8.RuneLen('你'))
}
