#!/bin/bash
dirpath=/home/distkv/jk/go_client_test/test #leveldb头文件和动态库所在位置根目录
export CGO_CFLAGS="-I$dirpath/leveldb/include -I$dirpath/snappy/include"
export CGO_LDFLAGS="-L$dirpath/leveldb/lib -L$dirpath/snappy/lib -lsnappy"
export LD_LIBRARY_PATH=$dirpath/leveldb/lib:$LD_LIBRARY_PATH