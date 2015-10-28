#!/usr/bin/env bash

# Should be run as root.

cd `dirname $0`

mkdir -p /usr/local/hdfs-cli/bin
mkdir -p /usr/local/hdfs-cli/lib

cp -f hdfscli /usr/local/hdfs-cli/bin
cp -f JCECheck /usr/local/hdfs-cli/bin

if [ -f ../target/hdfs-cli-full-bin.jar ]; then
    cp -f ../target/hdfs-cli-full-bin.jar /usr/local/hdfs-cli/lib
fi

if [ -f hdfs-cli-full-bin.jar ]; then
    cp -f hdfs-cli-full-bin.jar /usr/local/hdfs-cli/lib
fi

chmod -R +r /usr/local/hdfs-cli
chmod +x /usr/local/hdfs-cli/bin/hdfscli
chmod +x /usr/local/hdfs-cli/bin/JCECheck

ln -sf /usr/local/hdfs-cli/bin/JCECheck /usr/local/bin/JCECheck


