// Copyright 2021 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"testing"

<<<<<<< HEAD
	"github.com/JK1Zhang/client-go/v3/util"
=======
	"github.com/tikv/client-go/v2/util"
>>>>>>> 7683491695d090758b4274eccd76d6c975704324
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	util.EnableFailpoints()
	goleak.VerifyTestMain(m)
}
