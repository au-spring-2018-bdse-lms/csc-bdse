#!/bin/bash
set -u -e
VERSION=3.5.1
ARCHIVE=protoc-$VERSION-linux-x86_64.zip
wget https://github.com/google/protobuf/releases/download/v$VERSION/$ARCHIVE
unzip $ARCHIVE -d protoc
