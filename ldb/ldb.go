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
			keys := [][]byte{}
			vals := [][]byte{}
			for {
				if !buf.Scan() {
					break
				}
				line := buf.Text()
				line = strings.TrimSpace(line)
				str := strings.Fields(line)
				key := []byte(str[0])
				keys = append(keys, key)
				val := []byte(strings.Join(str[1:], " "))
				vals = append(vals, val)
				// err = cli.Put(context.TODO(), key, val)
				// if err != nil {
				// 	panic(err)
				// }
			}
			err = cli.BatchPut(context.TODO(), keys, vals)
			if err != nil {
				panic(err)
			}
		}
		fd.Close()
	}
	fmt.Printf("Write %d files!\n", len(files))
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
			if len(endval) != 0 {
				str = strings.Fields(string(endval))
				key = str[8] + " " + str[7]
				if _, ok := mapIP[key]; ok {
					mapIP[key]++
				} else {
					mapIP[key] = 1
				}
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
	for key, _ := range mapIP {
		data := key + "\n"
		fd.WriteString(data)
		num++
	}
	fmt.Printf("The number of different <ipv6.src,ipv6.dst> is : %d \n", num)
	fd.Close()
}


//LoadLSM 取两个时间戳区间内的所有 KV 对，并以流 ID (给出组成流ID的下标)为 key 重新生成键值存储
//以字典实现，分批次读取数据，每次最多从tikv中读取limit(10000)个以时间戳为key的<k,v>
//字典记录key，<k,v>写入数据库中，当有新数据时，若key不存在，直接写入，key存在就先读出再拼接写入
func LdbLoadLSM(cli *rawkv.Client, dbName, startTime, endTime string, flowIDPart []int) {
	limit := 10000
	startKey := []byte(startTime)
	endKey := []byte(endTime)
	mapIP := make(map[string]string)    //全局判断key是否已经存在
	mapIPTmp := make(map[string]string) //临时map，用来在scan一轮中将数据存储
	num := 0
	IDAll := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10} //所有属性的下标列表
	for i, id := range flowIDPart {                  //获取value对应的属性下标列表
		IDAll = append(IDAll[:id-i], IDAll[id-i+1:]...)
	}
	lenFlowID := len(flowIDPart)
	lenIDAll := len(IDAll)
	//打开leveldb数据库
	opt := levigo.NewOptions()
	opt.SetCreateIfMissing(true)
	db, err := levigo.Open(dbName, opt)
	if err != nil {
		fmt.Printf("open leveldb error!\n")
		return
	}
	wo := levigo.NewWriteOptions()
	ro := levigo.NewReadOptions()
	batch := levigo.NewWriteBatch()
	defer db.Close()
	defer wo.Close()
	defer ro.Close()
	defer batch.Close()
	//scan数据，转化后写入leveldb
	for {
		keyPart, valPart, err := cli.Scan(context.TODO(), startKey, endKey, limit)
		if err != nil || len(keyPart) != len(valPart) {
			panic(err)
		}
		num += len(keyPart)
		if len(valPart) < limit { //退出条件，加上右区间数据
			endVal, e := cli.Get(context.TODO(), endKey)
			if e != nil {
				panic(e)
			}
			if len(endVal) != 0 {
				valPart = append(valPart, endVal)
				keyPart = append(keyPart, endKey)
			}
			//当开始时数据为空则直接退出
			if len(valPart) == 0 {
				fmt.Printf("The <k,v> is nil between the range of timestamp!\n")
				return
			}
			//连着endkey对应的数据一起读出
			for i := 0; i < len(valPart); i++ {
				var keyBuilder strings.Builder
				var valBuilder strings.Builder
				str := strings.Fields(string(valPart[i]))

				//创建多属性构成的流ID key
				for j := 0; j < lenFlowID-1; j++ {
					keyBuilder.WriteString(str[flowIDPart[j]])
					keyBuilder.WriteByte(' ')
				}
				keyBuilder.WriteString(str[flowIDPart[lenFlowID-1]])
				//创建余下属性构成的value
				valBuilder.Write(keyPart[i])
				valBuilder.WriteByte(' ')
				for j := 0; j < lenIDAll-1; j++ {
					valBuilder.WriteString(str[IDAll[j]])
					valBuilder.WriteByte(' ')
				}
				valBuilder.WriteString(str[IDAll[lenIDAll-1]])

				key := keyBuilder.String()
				if value, ok := mapIPTmp[key]; ok {
					mapIPTmp[key] = value + "@" + valBuilder.String()
				} else {
					mapIPTmp[key] = valBuilder.String()
				}
				keyBuilder.Reset()
				valBuilder.Reset()
			}
			//写入数据库
			for key, val := range mapIPTmp {
				if _, ok := mapIP[key]; ok { //key已经存在
					value, err := db.Get(ro, []byte(key))
					if err != nil {
						fmt.Printf("read leveldb failed!\n")
						return
					}
					val = string(value) + "@" + val
				} else {
					mapIP[key] = ""
				}
				batch.Put([]byte(key), []byte(val))
			}
			mapIPTmp = make(map[string]string) //清空临时map
			err := db.Write(wo, batch)
			if err != nil {
				fmt.Printf("batchwrite leveldb failed!\n")
				return
			}
			batch.Clear()
			break
		} else {
			for i := 0; i < len(valPart)-1; i++ {
				var keyBuilder strings.Builder
				var valBuilder strings.Builder
				str := strings.Fields(string(valPart[i]))

				//创建多属性构成的流ID key
				for j := 0; j < lenFlowID-1; j++ {
					keyBuilder.WriteString(str[flowIDPart[j]])
					keyBuilder.WriteByte(' ')
				}
				keyBuilder.WriteString(str[flowIDPart[lenFlowID-1]])
				//创建余下属性构成的value
				valBuilder.Write(keyPart[i])
				valBuilder.WriteByte(' ')
				for j := 0; j < lenIDAll-1; j++ {
					valBuilder.WriteString(str[IDAll[j]])
					valBuilder.WriteByte(' ')
				}
				valBuilder.WriteString(str[IDAll[lenIDAll-1]])

				key := keyBuilder.String()
				if value, ok := mapIPTmp[key]; ok {
					mapIPTmp[key] = value + "@" + valBuilder.String()
				} else {
					mapIPTmp[key] = valBuilder.String()
				}
				keyBuilder.Reset()
				valBuilder.Reset()
			}
			//写入数据库
			for key, val := range mapIPTmp {
				if _, ok := mapIP[key]; ok { //key已经存在
					value, err := db.Get(ro, []byte(key))
					if err != nil {
						fmt.Printf("read leveldb failed!\n")
						return
					}
					val = string(value) + "@" + val
				} else {
					mapIP[key] = ""
				}
				batch.Put([]byte(key), []byte(val))
			}
			mapIPTmp = make(map[string]string) //清空临时map
			err := db.Write(wo, batch)
			if err != nil {
				fmt.Printf("batchwrite leveldb failed!\n")
				return
			}
			batch.Clear()
			startKey = keyPart[len(keyPart)-1]
		}
	}
	fmt.Printf("the total num of is : %d \n", num)
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
