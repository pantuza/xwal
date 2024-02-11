#!/bin/bash

GO=$(which go)
PROTOC=$(which protoc)

# The script is executed by Makefile, thereby path is relative to Makefile
SRC_DIR=protobuf/
DST_DIR=protobuf/

if [ -z "${PROTOC}" ]; then
	echo "protoc not found. Installing Protocol Buffers compiler.."
	brew install protobuf
	brew link --overwrite protobuf
	${GO} install google.golang.org/protobuf/cmd/protoc-gen-go@latest
fi

echo "Compiling Protocol Buffers.."
${PROTOC} -I=${SRC_DIR} --go_out=${DST_DIR} ${SRC_DIR}/wal_entry.proto && echo "OK"
