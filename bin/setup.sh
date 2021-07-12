#!/usr/bin/env sh

cd `dirname $0`

BASE_DIR=

if (( $EUID != 0 )); then
  echo "Setting up as non-root user"
  BASE_DIR=$HOME/.hadoop-cli
else
  echo "Setting up as root user"
  BASE_DIR=/usr/local/hadoop-cli
fi

# Install in User bin
mkdir -p $BASE_DIR/bin
mkdir -p $BASE_DIR/lib

cp -f hadoopcli $BASE_DIR/bin
cp -f JCECheck $BASE_DIR/bin

# Cleanup previous installation
rm -f $BASE_DIR/lib/*.jar
rm -f $BASE_DIR/bin/*.*

if [ -f ../target/hadoop-cli-shaded.jar ]; then
    cp -f ../target/hadoop-cli-shaded.jar $BASE_DIR/lib
fi

if [ -f ../target/hadoop-cli-shaded-no-hadoop.jar ]; then
    cp -f ../target/hadoop-cli-shaded-no-hadoop.jar $BASE_DIR/lib
fi

if [ -f hadoop-cli-shaded.jar ]; then
    cp -f hadoop-cli-shaded.jar $BASE_DIR/lib
fi

if [ -f hadoop-cli-shaded-no-hadoop.jar ]; then
    cp -f hadoop-cli-shaded-no-hadoop.jar $BASE_DIR/lib
fi


chmod -R +r $BASE_DIR
chmod +x $BASE_DIR/bin/hadoopcli
chmod +x $BASE_DIR/bin/JCECheck

if (( $EUID == 0 )); then
  echo "Setting up global links"
  ln -sf $BASE_DIR/bin/JCECheck /usr/local/bin/JCECheck
  ln -sf $BASE_DIR/bin/hadoopcli /usr/local/bin/hadoopcli
else
  mkdir -p $HOME/bin
  ln -sf $BASE_DIR/bin/JCECheck $HOME/bin/JCECheck
  ln -sf $BASE_DIR/bin/hadoopcli $HOME/bin/hadoopcli
fi
