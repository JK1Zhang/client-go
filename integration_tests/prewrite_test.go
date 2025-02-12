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

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/prewrite_test.go
//

// Copyright 2020 PingCAP, Inc.
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

package tikv_test

import (
	"testing"

<<<<<<< HEAD
	"github.com/JK1Zhang/client-go/v3/testutils"
	"github.com/JK1Zhang/client-go/v3/tikv"
	"github.com/JK1Zhang/client-go/v3/txnkv/transaction"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
=======
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
>>>>>>> 7683491695d090758b4274eccd76d6c975704324
)

func TestSetMinCommitTSInAsyncCommit(t *testing.T) {
	require, assert := require.New(t), assert.New(t)

	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.Nil(err)
	testutils.BootstrapWithSingleStore(cluster)
	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.Nil(err)
	defer store.Close()

	tx, err := store.Begin()
	require.Nil(err)
	txn := transaction.TxnProbe{KVTxn: tx}
	err = txn.Set([]byte("k"), []byte("v"))
	assert.Nil(err)
	committer, err := txn.NewCommitter(1)
	assert.Nil(err)
	committer.SetUseAsyncCommit()

	buildRequest := func() *kvrpcpb.PrewriteRequest {
		req := committer.BuildPrewriteRequest(1, 1, 1, committer.GetMutations(), 1)
		return req.Req.(*kvrpcpb.PrewriteRequest)
	}

	// no forUpdateTS
	req := buildRequest()
	assert.Equal(req.MinCommitTs, txn.StartTS()+1)

	// forUpdateTS is set
	committer.SetForUpdateTS(txn.StartTS() + (5 << 18))
	req = buildRequest()
	assert.Equal(req.MinCommitTs, committer.GetForUpdateTS()+1)

	// minCommitTS is set
	committer.SetMinCommitTS(txn.StartTS() + (10 << 18))
	req = buildRequest()
	assert.Equal(req.MinCommitTs, committer.GetMinCommitTS())

}
