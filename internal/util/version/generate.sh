#!/bin/sh

set -e

git describe --tags --dirty --always > version.txt
git branch --show-current > branch.txt