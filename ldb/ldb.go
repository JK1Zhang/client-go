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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
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
			fmt.Printf("open %s file error\n", file)
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
	fmt.Printf("the num of diff IP is : %d \n", num)
	fd.Close()
}

//LoadLSM 取两个时间戳区间内的所有 KV 对，并以流 ID (给出组成流ID的下标)为 key 重新生成键值存储
func LdbLoadLSM(cli *rawkv.Client, dbName, startTime, endTime string, flowIDPart []int) {
	limit := 10000
	startKey := []byte(startTime)
	endKey := []byte(endTime)
	mapIP := make(map[string]string)
	num := 0
	for {
		keyPart, valPart, err := cli.Scan(context.TODO(), startKey, endKey, limit)
		if err != nil || len(keyPart) != len(valPart) {
			panic(err)
		}
		if len(valPart) < limit { //退出条件
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
	fmt.Printf("the num of diff key is : %d \n", num)
}

//Get(FlowID): 根据FlowID获取对应流数据的 KV 对
func LdbGet(dbName, flowID string) (value []string, err error) {
	opt := levigo.NewOptions()
	db, err1 := levigo.Open(dbName, opt)
	if err != nil {
		fmt.Printf("open leveldb error!\n")
		return []string{}, err1
	}
	ro := levigo.NewReadOptions()
	defer db.Close()
	defer opt.Close()
	defer ro.Close()
	val, err2 := db.Get(ro, []byte(flowID))
	if err != nil {
		fmt.Printf("read the flowID from db error!\n")
		return []string{}, err2
	}
	if val == nil {
		fmt.Printf("the value is nil!\n")
	}
	str := strings.Split(string(val), "@")
	return str, nil
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
	defer db.Close()
	defer iter.Close()
	defer opt.Close()
	defer ro.Close()
	for iter.Seek([]byte(startFlowID)); iter.Valid() && string(iter.Key()) <= endFlowID; iter.Next() {
		str := strings.Split(string(iter.Value()), "@")
		for i := 0; i < len(str); i++ {
			key = append(key, string(iter.Key()))
			value = append(value, str[i])
		}

	}
	return key, value, nil
}

func GetGraph(cli *rawkv.Client, startTime, endTime string) {
	IPDirName := "./CSR"
	IPFileName := "./CSR/test"
	middleFile := "./CSR/test"
	fi, err := os.Stat(IPDirName)
	if err == nil && fi.IsDir() { //存在
		os.RemoveAll(IPDirName) //移除原有文件内容
	}
	os.Mkdir(IPDirName, 0777)
	os.Chmod(IPDirName, 0777)
	limit := 10000                                             //限制比rawkv.MaxRawKVScanLimit = 10240 小
	LdbLoadTXT(cli, IPFileName, startTime, endTime, limit) //产生中间文件
	cmd := exec.Command("ToCSR", "-g", middleFile)             //执行生成图文件的EXE程序
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout // 标准输出
	cmd.Stderr = &stderr // 标准错误
	err = cmd.Run()
	outStr, errStr := string(stdout.Bytes()), string(stderr.Bytes())
	fmt.Printf("stdout:\n%s\nstderr:\n%s\n", outStr, errStr)
	if err != nil {
		fmt.Printf("cmd.Run() failed with %s\n", err)
	}
	os.Remove(IPFileName) //删除中间文件
}
