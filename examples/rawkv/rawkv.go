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
	scanfile := "./test2.txt" //测试scan结果文件
	dbName := "./dbtest1"     //leveldb所在路径

	startkey := "1580274000.809441"
	endkey := "1580274003.012248"
	flowIDPart := []int{3, 7, 8}
	fmt.Println("选择的flowid下标为: ", flowIDPart)

	//测试GetGraph
	ldb.GetGraph(cli, startkey, endkey)

	//测试loadLSM，保存流 ID kv对到levelDB
	ldb.LdbLoadLSM(cli, dbName, startkey, endkey, flowIDPart)

	//测试get
	IDkey := "525394 9e60:10ae:88aa:a676:1023:450b:d646:3079 406c:3fdb:55d5:ba4f:be10:6c78:f45c:674d"
	val, err := ldb.LdbGet(dbName, IDkey)
	if err != nil {
		fmt.Printf("get key  from db error\n")
		return
	}
	fd1, err := os.OpenFile(Getfile, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("open txt file error!\n")
		return
	}
	defer fd1.Close()
	fd1.WriteString("key :  " + IDkey + "\n" + "Value :\n")
	for i := 0; i < len(val); i++ {
		value := val[i] + "\n"
		fd1.WriteString(value)
	}
	fmt.Printf("the num of value is : %d\n", len(val))

	//测试scan
	startTime := "0 cc91:d473:646e:513a:1261:f28a:4a94:3a66 f71c:4fa5:6144:546b:2a63:406f:1d92:a7a0"
	endTime := "525394 9e60:10ae:88aa:a676:1023:450b:d646:3079 406c:3fdb:55d5:ba4f:be10:6c78:f45c:674d"
	keys, vals, err := ldb.LdbScan(dbName, startTime, endTime)
	if err != nil {
		fmt.Printf("get key  from db error\n")
		return
	}
	fd2, err := os.OpenFile(scanfile, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("open txt file error!\n")
		return
	}
	defer fd2.Close()
	for i := 0; i < len(vals); i++ {
		fd2.WriteString("key : " + keys[i] + "  " + "Value : " + vals[i])
		fd2.WriteString("\n")
	}
	fmt.Printf("the num of kv is : %d\n", len(vals))

	// //fmt.Printf("read ipv6 file！\n")
	//dirName := "/home/distkv/jk/go_client_test/ipv6/6_2"
	// dirName3 := "/home/distkv/jk/go_client_test/ipv6/6_3"
	// dirName4 := "/home/distkv/jk/go_client_test/ipv6/6_4"
	// dirName5 := "/home/distkv/jk/go_client_test/ipv6/6_5"
	// //files := ListFile(dirName)
	// //WriteFile(cli, files)
	// dirName := [][]string{}
	// files3 := ListFile(dirName3)
	// files4 := ListFile(dirName4)
	// files5 := ListFile(dirName5)
	// dirName = append(dirName, files3)
	// dirName = append(dirName, files4)
	// dirName = append(dirName, files5)
	// wg := sync.WaitGroup{}
	// for i := 0; i < 3; i++ {
	// 	wg.Add(1)
	// 	go func(cli *rawkv.Client, dirname []string) {
	// 		WriteFile(cli, dirname)
	// 		wg.Done()
	// 	}(cli, dirName[i])
	// }
	// wg.Wait()
	// fmt.Printf("finish\n")
}
