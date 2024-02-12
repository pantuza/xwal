#!/bin/bash

PKGMGR=$(which brew)
GO=$(which go)

echo "Installs operating system level tools and dependencies.."
${PKGMGR} install \
	golangci-lint \
	protobuf

echo "Linking needed tools and dependencies.."
brew link --overwrite \
	protobuf

echo "Installing golang plugins.."
${GO} install \
	google.golang.org/protobuf/cmd/protoc-gen-go@latest
