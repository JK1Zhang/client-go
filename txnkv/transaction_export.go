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

package txnkv

<<<<<<< HEAD
import "github.com/JK1Zhang/client-go/v3/txnkv/transaction"
=======
import "github.com/tikv/client-go/v2/txnkv/transaction"
>>>>>>> 7683491695d090758b4274eccd76d6c975704324

// KVTxn contains methods to interact with a TiKV transaction.
type KVTxn = transaction.KVTxn

// BinlogWriteResult defines the result of prewrite binlog.
type BinlogWriteResult = transaction.BinlogWriteResult

// KVFilter is a filter that filters out unnecessary KV pairs.
type KVFilter = transaction.KVFilter

// SchemaLeaseChecker is used to validate schema version is not changed during transaction execution.
type SchemaLeaseChecker = transaction.SchemaLeaseChecker

// SchemaVer is the infoSchema which will return the schema version.
type SchemaVer = transaction.SchemaVer

// SchemaAmender is used by pessimistic transactions to amend commit mutations for schema change during 2pc.
type SchemaAmender = transaction.SchemaAmender

// MaxTxnTimeUse is the max time a Txn may use (in ms) from its begin to commit.
// We use it to abort the transaction to guarantee GC worker will not influence it.
const MaxTxnTimeUse = transaction.MaxTxnTimeUse
