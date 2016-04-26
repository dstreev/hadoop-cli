#!/usr/bin/env bash

# Should be run as root.

cd `dirname $0`

mkdir -p /usr/local/hadoop-cli/bin
mkdir -p /usr/local/hadoop-cli/lib

cp -f hadoopcli /usr/local/hadoop-cli/bin
cp -f JCECheck /usr/local/hadoop-cli/bin

if [ -f ../target/hadoop-cli-full-bin.jar ]; then
    cp -f ../target/hadoop-cli-full-bin.jar /usr/local/hadoop-cli/lib
fi

if [ -f hadoop-cli-full-bin.jar ]; then
    cp -f hadoop-cli-full-bin.jar /usr/local/hadoop-cli/lib
fi

chmod -R +r /usr/local/hadoop-cli
chmod +x /usr/local/hadoop-cli/bin/hadoopcli
chmod +x /usr/local/hadoop-cli/bin/JCECheck

ln -sf /usr/local/hadoop-cli/bin/JCECheck /usr/local/bin/JCECheck
ln -sf /usr/local/hadoop-cli/bin/hadoopcli /usr/local/bin/hadoopcli


