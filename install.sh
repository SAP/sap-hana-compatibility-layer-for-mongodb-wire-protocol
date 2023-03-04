#!/bin/bash

current_dir="$(pwd)"

echo $current_dir

mkdir tmpDir 
ls
echo 1
curl https://tools.hana.ondemand.com/additional/hanaclient-latest-linux-x64.tar.gz --output tmpDir/hanaclient.tar.gz 
echo 2
tar -xzvf tmpDir/hanaclient.tar.gz -C tmpDir 
cd tmpDir
ls
cd ..
echo 3
tmpDir/client/./hdbinst --batch --ignore=check_diskspace
echo 4
mv /home/runner/sap/hdbclient/golang/src/SAP /opt/hostedtoolcache/go/1.20.1/x64/src/
echo 5
cd home/runner/sap/hdbclient/golang/src/ 
echo 6
go install SAP/go-hdb/driver
echo 7

export PATH=$PATH:/home/runner/sap/hdbclient
export CGO_LDFLAGS=/home/runner/sap/hdbclient/libdbcapiHDB.so
export GO111MODULE=auto
export LD_LIBRARY_PATH=/home/runner/hdbclient/