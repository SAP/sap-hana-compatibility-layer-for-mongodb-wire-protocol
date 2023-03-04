#!/bin/bash

work_dir=$(dirname $(dirname "$(pwd)"))
echo $work_dir

cd "${work_dir}"

ls