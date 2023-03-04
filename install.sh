#!/bin/bash

work_dir=$(dirname $(dirname "$(pwd)"))
echo $work_dir

cd "${work_dir}"

mkdir hanaDriver

curl https://tools.hana.ondemand.com/additional/hanaclient-latest-linux-x64.tar.gz -H 'Cookie: eula_3_1_agreed=tools.hana.ondemand.com/developer-license-3_1.txt'  --output hanaDriver/hanaclient.tar.gz

tar -xzvf hanaDriver/hanaclient.tar.gz -C hanaDriver

hanaDriver/client/./hdbinst --batch --ignore=check_diskspace
ls
cd ..
ls 
cd ..
ls
mv "${work_dir}"hanaDriver/golang/src/SAP /opt/hostedtoolcache/go/1.20.1/x64/src/

cd "${work_dir}"hanaDriver/golang/src/ 

go install SAP/go-hdb/driver


export PATH=$PATH:/home/runner/sap/hdbclient
export CGO_LDFLAGS=/home/runner/sap/hdbclient/libdbcapiHDB.so
export GO111MODULE=auto
export LD_LIBRARY_PATH=/home/runner/hdbclient/