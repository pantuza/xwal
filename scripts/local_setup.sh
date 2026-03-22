#!/bin/bash

PKGMGR=$(which brew)
GO=$(which go)

echo "Installs operating system level tools and dependencies.."
${PKGMGR} install \
	protobuf

echo "Linking needed tools and dependencies.."
brew link --overwrite \
	protobuf

echo "Installing golangci-lint v2 (built with Go 1.26 so it can lint this module).."
GOTOOLCHAIN=go1.26.0 ${GO} install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.4

echo "Installing golang plugins.."
${GO} install \
	google.golang.org/protobuf/cmd/protoc-gen-go@latest
