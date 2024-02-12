#!/bin/bash

PROTOC=$(which protoc)

# The script is executed by Makefile, thereby path is relative to Makefile
SRC_DIR=protobuf/
DST_DIR=protobuf/

if [ -z "${PROTOC}" ]; then
	echo "protoc compiler not found. Please, run 'make setup' to configure your local machine to use this project."
	exit 1
fi

echo "Compiling Protocol Buffers.."
${PROTOC} -I=${SRC_DIR} --go_out=${DST_DIR} ${SRC_DIR}/wal_entry.proto && echo "OK"
