#!/usr/bin/env bash

# Should be run as root.

cd `dirname $0`

mkdir -p /usr/local/hdfs-cli/bin
mkdir -p /usr/local/hdfs-cli/lib

cp -f hdfscli /usr/local/hdfs-cli/bin
cp -f ../target/hdfs-cli-full-bin.jar /usr/local/hdfs-cli/lib

chmod -R +r /usr/local/hdfs-cli
chmod +x /usr/local/hdfs-cli/bin/hdfscli

ln -sf /usr/local/hdfs-cli/bin/hdfscli /usr/local/bin/hdfscli


