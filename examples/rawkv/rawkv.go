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

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/JK1Zhang/client-go/v3/config"
	"github.com/JK1Zhang/client-go/v3/ldb"
	"github.com/JK1Zhang/client-go/v3/rawkv"
)

func main() {
	cli, err := rawkv.NewClient(context.TODO(), []string{"127.0.0.1:2479"}, config.DefaultConfig().Security)
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	Getfile := "./test1.txt"  //测试get结果文件
	dbName := "./dbtest1"     //leveldb所在路径
	startkey := "1580274000"
	endkey := "1580274001"
	IDKeyIpv4 := []int{5, 9, 10}
	IDKeyIpv6 := []int{3, 7, 8}
	fmt.Println("startkey: ", startkey)
	fmt.Println("endkey: ", endkey)


	//测试GetGraph
	ldb.GetGraph(cli, startkey, endkey)

	//测试loadLSM，保存流 ID kv对到levelDB
	ldb.LdbLoadLSM(cli, dbName, startkey, endkey, IDKeyIpv4, IDKeyIpv6)

	//测试get
	IDkey1 := "3326 98.218.18.85 103.72.105.164"
	val1, err1 := ldb.LdbGet(dbName, IDkey1)
	if err2 != nil {
		fmt.Printf("get key  from db error\n")
		return
	}
	IDkey2 := "525394 9e60:10ae:88aa:a676:1023:450b:d646:3079 406c:3fdb:55d5:ba4f:be10:6c78:f45c:674d"
	val2, err2 := ldb.LdbGet(dbName, IDkey2)
	if err2 != nil {
		fmt.Printf("get key  from db error\n")
		return
	}
	fd1, err := os.OpenFile(Getfile, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("open txt file error!\n")
		return
	}
	defer fd1.Close()
	fd1.WriteString("key :  " + IDkey1 + "\n" + "Value :\n")
	for i := 0; i < len(val1); i++ {
		value := val1[i] + "\n"
		fd1.WriteString(value)
	}
	fd1.WriteString("key :  " + IDkey2 + "\n" + "Value :\n")
	for i := 0; i < len(val2); i++ {
		value := val2[i] + "\n"
		fd1.WriteString(value)
	}

	//测试scan
	startTime := "0"
	endTime := "1"
	keys, vals, err := ldb.LdbScan(dbName, startTime, endTime)
	if err != nil {
		fmt.Printf("get key  from db error\n")
		return
	}
	fmt.Printf("%d\n", len(vals))
	fmt.Printf("%d\n", len(keys))
}
