#!/bin/bash



if [ "$1" != "install" ];
then
    echo "no"
    exit 1
fi

work_dir=$(dirname $(dirname "$(pwd)"))
back_dir=$(dirname "${work_dir}")
echo $work_dir
echo $back_dir

cd "${work_dir}"

prefix="GOROOT=\""
suffix="\""
gorootStr="$(go env | grep GOROOT)"
goroot=${gorootStr#"$prefix"}
goroot=${goroot%"$suffix"}
echo $goroot

ls