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

package ldb

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/JK1Zhang/client-go/v3/rawkv"
	"github.com/jmhodges/levigo"
)

//读取目录下的所有文件名
func LdbListFile(dirName string) (files []string) {
	dir, err := ioutil.ReadDir(dirName)
	if err != nil {
		return nil
	}

	for _, fi := range dir {
		if fi.IsDir() { // 目录, 递归遍历
			filename := dirName + "/" + fi.Name()
			files = append(files, LdbListFile(filename)...)
		} else {
			ok := strings.HasSuffix(fi.Name(), ".txt")
			if ok {
				files = append(files, dirName+"/"+fi.Name())
			}
		}
	}

	return files
}

//将所有文件写入数据库
func LdbWriteFile(cli *rawkv.Client, files []string) {
	for _, file := range files {
		fd, err := os.Open(file)
		if err != nil {
			fmt.Println("open %s file error\n", file)
		} else {
			buf := bufio.NewScanner(fd)
			for {
				if !buf.Scan() {
					break
				}
				line := buf.Text()
				line = strings.TrimSpace(line)
				str := strings.Fields(line)
				key := []byte(str[0])
				val := []byte(strings.Join(str[1:], " "))
				err = cli.Put(context.TODO(), key, val)
				if err != nil {
					panic(err)
				}
			}
		}
		fmt.Println(file)
		fd.Close()
	}
}

//导出数据库中的所有文件的ip, limit为每次scan的长度，区间[startTime，endTime]
func LdbLoadTXT(cli *rawkv.Client, fileName, startTime, endTime string, limit int) {
	startKey := []byte(startTime)
	endKey := []byte(endTime)
	mapIP := make(map[string]int)
	num := 0
	if limit > rawkv.MaxRawKVScanLimit {
		fmt.Printf("limit is invalid! change default\n")
		limit = rawkv.MaxRawKVScanLimit
	}
	for {
		keyPart, valPart, err := cli.Scan(context.TODO(), startKey, endKey, limit)
		if err != nil || len(keyPart) != len(valPart) {
			panic(err)
		}
		for i := 0; i < len(valPart)-1; i++ {
			val := string(valPart[i])
			str := strings.Fields(val)
			key := str[8] + " " + str[7]
			if _, ok := mapIP[key]; ok {
				//存在ipv6.src dst地址时，直接增加计数
				mapIP[key]++
			} else {
				mapIP[key] = 1
			}
		}
		startKey = keyPart[len(valPart)-1]
		if len(valPart) < limit {
			val := string(valPart[len(valPart)-1])
			str := strings.Fields(val)
			key := str[8] + " " + str[7]
			if _, ok := mapIP[key]; ok {
				mapIP[key]++
			} else {
				mapIP[key] = 1
			}
			endval, err := cli.Get(context.TODO(), endKey)
			if err != nil {
				panic(err)
			}
			//endkey对应的val读出
			str = strings.Fields(string(endval))
			key = str[8] + " " + str[7]
			if _, ok := mapIP[key]; ok {
				mapIP[key]++
			} else {
				mapIP[key] = 1
			}
			break
		}
	}
	//写到文件中
	fd, err := os.OpenFile(fileName, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("open txt file error!\n")
		return
	}
	//遍历字典
	for key, val := range mapIP {
		data := key + "\n"
		fd.WriteString(data)
		num += val
	}
	fmt.Println(num)
	fd.Close()
}

//LoadLSM 取两个时间戳区间内的所有 KV 对，并以流 ID (给出组成流ID的下标)为 key 重新生成键值存储
func LdbLoadLSM(cli *rawkv.Client, dbName, startTime, endTime string, flowIDPart []int) {
	limit := 1000
	startKey := []byte(startTime)
	endKey := []byte(endTime)
	mapIP := make(map[string]string)
	num := 0
	for {
		keyPart, valPart, err := cli.Scan(context.TODO(), startKey, endKey, limit)
		if err != nil || len(keyPart) != len(valPart) {
			panic(err)
		}
		if len(valPart) < limit {
			endVal, e := cli.Get(context.TODO(), endKey)
			if e != nil {
				panic(e)
			}
			valPart = append(valPart, endVal)
			keyPart = append(keyPart, endKey)
			//连着endkey对应的数据一起读出
			for i := 0; i < len(valPart); i++ {
				str := strings.Fields(string(valPart[i]))
				val := string(keyPart[i]) + " " + strings.Join(str[0:flowIDPart[0]], " ")
				key := str[flowIDPart[0]]
				j := 1
				for ; j < len(flowIDPart); j++ {
					key += " " + str[flowIDPart[j]]
					val += " " + strings.Join(str[flowIDPart[j-1]+1:flowIDPart[j]], " ")
				}
				val += " " + strings.Join(str[flowIDPart[j-1]+1:], " ")
				if value, ok := mapIP[key]; ok {
					mapIP[key] = value + "@" + val
				} else {
					mapIP[key] = val
				}
			}
			break
		} else {
			for i := 0; i < len(valPart)-1; i++ {
				str := strings.Fields(string(valPart[i]))
				val := string(keyPart[i]) + " " + strings.Join(str[0:flowIDPart[0]], " ")
				key := str[flowIDPart[0]]
				j := 1
				for ; j < len(flowIDPart); j++ {
					key += " " + str[flowIDPart[j]]
					val += " " + strings.Join(str[flowIDPart[j-1]+1:flowIDPart[j]], " ")
				}
				val += " " + strings.Join(str[flowIDPart[j-1]+1:], " ")
				if value, ok := mapIP[key]; ok {
					mapIP[key] = value + "@" + val
				} else {
					mapIP[key] = val
				}
				startKey = keyPart[len(keyPart)-1]
			}
		}
	}
	//遍历字典，写到leveldb中
	opt := levigo.NewOptions()
	opt.SetCreateIfMissing(true)
	db, err := levigo.Open(dbName, opt)
	if err != nil {
		fmt.Printf("open leveldb error!\n")
		return
	}
	wo := levigo.NewWriteOptions()
	batch := levigo.NewWriteBatch()
	defer db.Close()
	defer wo.Close()
	defer batch.Close()
	for key, val := range mapIP {
		batch.Put([]byte(key), []byte(val))
		num++
		if num%100 == 0 {
			db.Write(wo, batch)
			batch.Clear()
		}
	}
	db.Write(wo, batch)
	batch.Clear()
	fmt.Println(num)
}

//Get(FlowID): 根据FlowID获取对应流数据的 KV 对
func LdbGet(dbName, flowID string) (value string, err error) {
	opt := levigo.NewOptions()
	db, err := levigo.Open(dbName, opt)
	if err != nil {
		fmt.Printf("open leveldb error!\n")
		return "", err
	}
	ro := levigo.NewReadOptions()
	defer ro.Close()
	defer db.Close()
	val, err := db.Get(ro, []byte(flowID))
	if err != nil {
		fmt.Printf("read the flowID from db error!\n")
	}
	if val == nil {
		fmt.Printf("the value is nil!")
	}
	return string(val), nil
}

//Scan( FlowID _start, FlowID _end): 获取两个流 ID 区间内(字母序)的所有流数据
func LdbScan(dbName, startFlowID, endFlowID string) (key []string, value []string, err error) {
	opt := levigo.NewOptions()
	db, err := levigo.Open(dbName, opt)
	if err != nil {
		fmt.Printf("open leveldb error!\n")
		return []string{}, []string{}, err
	}
	ro := levigo.NewReadOptions()
	iter := db.NewIterator(ro)
	defer ro.Close()
	defer iter.Close()
	defer db.Close()
	for iter.Seek([]byte(startFlowID)); iter.Valid() && string(iter.Key()) <= endFlowID; iter.Next() {
		key = append(key, string(iter.Key()))
		value = append(value, string(iter.Value()))
	}
	return key, value, nil
}
