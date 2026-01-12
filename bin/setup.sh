#!/usr/bin/env sh

#
# Copyright (c) 2022. David W. Streever All Rights Reserved
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

cd `dirname $0`

BASE_DIR=

if (( $EUID != 0 )); then
  echo "Setting up as non-root user"
  BASE_DIR=$HOME/.hcfs-cli
else
  echo "Setting up as root user"
  BASE_DIR=/usr/local/hcfs-cli
fi

# Install in User bin
mkdir -p $BASE_DIR/bin
mkdir -p $BASE_DIR/lib

# Cleanup previous installation
rm -f $BASE_DIR/lib/*.jar
rm -f $BASE_DIR/bin/*.*

cp -f hcfs-cli $BASE_DIR/bin

for jar in `ls lib/*.jar`; do
    cp -f $jar $BASE_DIR/lib
done

chmod -R +r $BASE_DIR
chmod +x $BASE_DIR/bin/hcfs-cli

if (( $EUID == 0 )); then
  echo "Setting up global links"
  ln -sf $BASE_DIR/bin/hcfs-cli /usr/local/bin/hcfs-cli
else
  mkdir -p $HOME/bin
  ln -sf $BASE_DIR/bin/hcfs-cli $HOME/bin/hcfs-cli
fi
