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

type Product struct {
	mapIP   map[string]string
	kvnum   int //map中的key数量
	lastkey string
}
type Tm struct {
	parseTime float64
	scanTime  float64
}

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
			var key string
			val := string(valPart[i])
			str := strings.Fields(val)
			if len(str) == 11 {
				key = str[8] + " " + str[7]
			} else if len(str) == 13 {
				key = str[10] + " " + str[9]
			} else {
				continue
			}
			if _, ok := mapIP[key]; ok {
				//存在src dst地址时，直接增加计数
				mapIP[key]++
			} else {
				mapIP[key] = 1
			}
		}
		startKey = keyPart[len(valPart)-1]
		if len(valPart) < limit {
			var key string
			val := string(valPart[len(valPart)-1])
			str := strings.Fields(val)
			if len(str) == 11 {
				key = str[8] + " " + str[7]
			} else if len(str) == 13 {
				key = str[10] + " " + str[9]
			} else {
				goto readendval
			}
			if _, ok := mapIP[key]; ok {
				mapIP[key]++
			} else {
				mapIP[key] = 1
			}
		readendval:
			endval, err := cli.Get(context.TODO(), endKey)
			if err != nil {
				panic(err)
			}
			//endkey对应的val读出
			if len(endval) != 0 {
				str = strings.Fields(string(endval))
				if len(str) == 11 {
					key = str[8] + " " + str[7]
				} else if len(str) == 13 {
					key = str[10] + " " + str[9]
				} else {
					goto bk
				}
				if _, ok := mapIP[key]; ok {
					mapIP[key]++
				} else {
					mapIP[key] = 1
				}
			}
		bk:
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
	fmt.Printf("the num of diff IP is : %d \n", num)
	fd.Close()
}

func ParseStr(keyPart, valPart [][]byte, IDnum [][]int) (mapIP map[string]string) {
	var keyBuilder strings.Builder
	var valBuilder strings.Builder
	var IDkey []int
	var IDval []int
	var lenIDkey int
	var lenIDval int
	lenvalpart := len(valPart)
	mapIP = make(map[string]string)

	for i := 0; i < lenvalpart; i++ {
		str := strings.Fields(string(valPart[i]))
		if len(str) == 11 {
			IDkey = IDnum[1]
			IDval = IDnum[3]
			lenIDkey = len(IDnum[1])
			lenIDval = len(IDnum[3])
		} else if len(str) == 13 {
			IDkey = IDnum[0]
			IDval = IDnum[2]
			lenIDkey = len(IDnum[0])
			lenIDval = len(IDnum[2])
		} else {
			continue //数据既不是ipv4，也不是ipv6，丢弃
		}
		//创建多属性构成的流ID key
		for j := 0; j < lenIDkey-1; j++ {
			keyBuilder.WriteString(str[IDkey[j]])
			keyBuilder.WriteByte(' ')
		}
		keyBuilder.WriteString(str[IDkey[lenIDkey-1]])
		//创建余下属性构成的value
		valBuilder.Write(keyPart[i])
		valBuilder.WriteByte(' ')
		for j := 0; j < lenIDval-1; j++ {
			valBuilder.WriteString(str[IDval[j]])
			valBuilder.WriteByte(' ')
		}
		valBuilder.WriteString(str[IDval[lenIDval-1]])

		key := keyBuilder.String()
		val := valBuilder.String()
		if value, ok := mapIP[key]; ok {
			mapIP[key] = value + "@" + val
		} else {
			mapIP[key] = val
		}
		keyBuilder.Reset()
		valBuilder.Reset()
	}
	return mapIP
}

func produceScan(cli *rawkv.Client, timeRange []string, IDKey [][]int, Ip chan<- Product, TM chan<- Tm, wg *sync.WaitGroup) {
	limit := 10000
	startKey := []byte(timeRange[0])
	endKey := []byte(timeRange[1])
	parseT := float64(0) //字符串解析时间
	scanT := float64(0)  //scan时间
	num := 0

	flag := 0 //for循环退出标志
	mapIPTmp := make(map[string]string)
	keyPart := [][]byte{}
	valPart := [][]byte{}

	//scan数据
	for {
		for i := 0; i < 3; i++ {
			t1 := time.Now()
			keytmp, valtmp, err := cli.Scan(context.TODO(), startKey, endKey, limit)
			if err != nil || len(keytmp) != len(valtmp) {
				panic(err)
			}
			if len(valtmp) < limit { //退出小循环，和大循环
				endVal, e := cli.Get(context.TODO(), endKey)
				if e != nil {
					panic(e)
				}
				if len(endVal) != 0 {
					valtmp = append(valtmp, endVal)
					keytmp = append(keytmp, endKey)
				}
				keyPart = append(keyPart, keytmp...)
				valPart = append(valPart, valtmp...)
				flag = 1
				break
			} else {
				keyPart = append(keyPart, keytmp[:len(keytmp)-1]...)
				valPart = append(valPart, valtmp[:len(valtmp)-1]...)
				startKey = keytmp[len(keytmp)-1]
			}
			t2 := time.Now()
			scanT += t2.Sub(t1).Minutes()
		}
		//将数据写到临时 map 中
		t1 := time.Now()
		mapIPTmp = ParseStr(keyPart, valPart, IDKey)
		t2 := time.Now()
		parseT += t2.Sub(t1).Minutes()
		num += len(valPart)
		if flag == 1 { //数据scan完毕
			Ip <- Product{mapIP: mapIPTmp, kvnum: num, lastkey: string(startKey)}
			break
		} else {
			Ip <- Product{mapIP: mapIPTmp, kvnum: num, lastkey: string(startKey)}
			mapIPTmp = make(map[string]string) //清空数据
			keyPart = [][]byte{}
			valPart = [][]byte{}
			num = 0
		}
	}
	TM <- Tm{parseTime: parseT, scanTime: scanT}
	wg.Done()
}

//LoadLSM 取两个时间戳区间内的所有 KV 对，并以流 ID (给出组成流ID的下标)为 key 重新生成键值存储
//以字典实现，分批次读取数据，每次最多从tikv中读取limit(10000)个以时间戳为key的<k,v>
//字典记录key，<k,v>写入数据库中，当有新数据时，若key不存在，直接写入，key存在就先读出再拼接写入
func LdbLoadLSM(cli *rawkv.Client, dbName, startTime, endTime string, IDKeyIpv4 []int, IDKeyIpv6 []int) {
	//打开leveldb数据库
	opt := levigo.NewOptions()
	opt.SetCreateIfMissing(true)
	opt.SetWriteBufferSize(67108864)
	policy := levigo.NewBloomFilter(10)
	opt.SetFilterPolicy(policy)
	db, err := levigo.Open(dbName, opt)
	if err != nil {
		fmt.Printf("open leveldb error!\n")
		return
	}
	wo := levigo.NewWriteOptions()
	ro := levigo.NewReadOptions()
	batch1 := levigo.NewWriteBatch()
	batch2 := levigo.NewWriteBatch()
	batch3 := levigo.NewWriteBatch()

	IDValIpv4 := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	IDValIpv6 := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for i, id := range IDKeyIpv4 {
		IDValIpv4 = append(IDValIpv4[:id-i], IDValIpv4[id-i+1:]...)
	}
	for i, id := range IDKeyIpv6 {
		IDValIpv6 = append(IDValIpv6[:id-i], IDValIpv6[id-i+1:]...)
	}
	IDKey := [][]int{IDKeyIpv4, IDKeyIpv6, IDValIpv4, IDValIpv6}

	timePoints := []string{}
	timePoints = append(timePoints, startTime)
	startfloat, _ := strconv.ParseFloat(startTime, 64)
	endfloat, _ := strconv.ParseFloat(endTime, 64)
	mid1float := startfloat + (endfloat-startfloat)/3
	mid2float := mid1float + (endfloat-startfloat)/3
	mid1Time := strconv.FormatFloat(mid1float, 'f', 6, 64)
	mid2Time := strconv.FormatFloat(mid2float, 'f', 6, 64)
	timePoints = append(timePoints, mid1Time)
	timePoints = append(timePoints, mid2Time)
	timePoints = append(timePoints, endTime)

	IP := make(chan Product, 10)
	TM := make(chan Tm, 3)
	batch := []*levigo.WriteBatch{batch1, batch2, batch3}
	numt := 0
	parset := float64(0)
	scant := float64(0)
	loadt := float64(0)

	wgp := sync.WaitGroup{}
	wgc := sync.WaitGroup{}
	//生产者
	for i := 0; i < 3; i++ {
		wgp.Add(1)
		fmt.Printf("time range: %s-%s\n", timePoints[i], timePoints[i+1])
		go produceScan(cli, []string{timePoints[i], timePoints[i+1]}, IDKey, IP, TM, &wgp)
	}

	//消费者
	t1 := time.Now()
	for i := 0; i < 3; i++ {
		wgc.Add(1)
		go func(i int) {
			j := 0
			fd, err := os.OpenFile("./file"+fmt.Sprintf("%d", i), os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
			if err != nil {
				fmt.Printf("open file error!\n")
				return
			}
			defer fd.Close()
			for pro := range IP { //写入数据库
				j++
				numt += pro.kvnum
				for key, val := range pro.mapIP {
					value, err := db.Get(ro, []byte(key))
					if err != nil {
						fmt.Printf("read leveldb failed!\n")
						return
					}
					if len(value) != 0 { //已存在
						val = string(value) + "@" + val
						batch[i].Put([]byte(key), []byte(val))
					} else {
						batch[i].Put([]byte(key), []byte(val))
					}
				}
				err := db.Write(wo, batch[i])
				if err != nil {
					fmt.Printf("batchwrite leveldb failed!\n")
					return
				}
				batch[i].Clear()
				if j%30 == 0 {
					fd.WriteString("already write to timestamp: " + pro.lastkey + "\n")
				}
			}
			wgc.Done()
		}(i)
	}

	wgp.Wait()
	close(IP)
	close(TM)
	wgc.Wait()
	for tm := range TM {
		parset += tm.parseTime
		scant += tm.scanTime
	}
	t2 := time.Now()
	loadt += t2.Sub(t1).Minutes()
	fmt.Printf("kv time num: %d\n", numt)
	fmt.Printf("scan time: %f\n", scant)
	fmt.Printf("parse time: %f\n", parset)
	fmt.Printf("load time: %f\n", loadt)
	db.Close()
	wo.Close()
	ro.Close()
	batch1.Close()
	batch2.Close()
	batch3.Close()
	policy.Close()
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
