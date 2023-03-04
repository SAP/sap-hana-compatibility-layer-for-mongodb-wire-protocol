#!/bin/bash

work_dir=$(dirname $(dirname "$(pwd)"))
echo $work_dir

prefix="GOROOT=\""
suffix="\""
gorootStr="$(go env | grep GOROOT)"
goroot=${gorootStr#"$prefix"}
goroot=${goroot%"$suffix"}
echo $goroot

cd "${work_dir}"

mkdir hanaDriver

curl https://tools.hana.ondemand.com/additional/hanaclient-latest-linux-x64.tar.gz -H 'Cookie: eula_3_1_agreed=tools.hana.ondemand.com/developer-license-3_1.txt'  --output hanaDriver/hanaclient.tar.gz

tar -xzvf hanaDriver/hanaclient.tar.gz -C hanaDriver

hanaDriver/client/./hdbinst --batch --ignore=check_diskspace

install_dir=$(dirname "${work_dir}")

sudo mv "${install_dir}"/sap/hdbclient/golang/src/SAP "${goroot}"/src/

cd "${install_dir}"/sap/hdbclient/golang/src

go install SAP/go-hdb/driver


export PATH=$PATH:"${install_dir}"/sap/hdbclient
export CGO_LDFLAGS="${install_dir}"/sap/hdbclient/libdbcapiHDB.so
export GO111MODULE=auto
export LD_LIBRARY_PATH="${install_dir}"/sap/hdbclient