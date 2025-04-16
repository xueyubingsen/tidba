#!/bin/bash

set -e

# Compiled Platform
platforms=("linux/amd64" "linux/arm64")

GO_VERSION_MAJOR=$(go version | sed -e 's/.*go\([0-9]\+\)\..*/\1/')
GO_VERSION_MINOR=$(go version | sed -e 's/.*go[0-9]\+\.\([0-9]\+\)\..*/\1/')

echo "Major golang version [$GO_VERSION_MAJOR] minor golang version [$GO_VERSION_MINOR]"

# Prepare build env
REPO="github.com/wentaojin/tidba"
COMMIT=$(git describe --always --no-match --tags --dirty="-dev")
BUILDTS=$(date '+%Y-%m-%d %H:%M:%S')
GITHASH=$(git rev-parse HEAD)
GITREF=$(git rev-parse --abbrev-ref HEAD)

LDFLAGS="-w -s"

# Add build flags
LDFLAGS+=" -X '$REPO/version.Version=$COMMIT'"
LDFLAGS+=" -X '$REPO/version.BuildTS=$BUILDTS'"
LDFLAGS+=" -X '$REPO/version.GitHash=$GITHASH'"
LDFLAGS+=" -X '$REPO/version.GitBranch=$GITREF'"

echo "Prepare build flags [$LDFLAGS]"

# Go main path
APP_SRC=$(pwd)
TiDBA="$APP_SRC/main.go"


# Compiled output
LINUX_AMD64_DIR="$APP_SRC/linux/amd64"
LINUX_ARM64_DIR="$APP_SRC/linux/arm64"

mkdir -p $LINUX_AMD64_DIR
mkdir -p $LINUX_AMD64_DIR

for platform in "${platforms[@]}"; do
    IFS='/' read -r XGOOS XGOARCH <<< "$platform"

    echo "Compiling for $XGOOS/$XGOARCH..."

    if { [ "$XGOOS" == "." ] || [ "$XGOOS" == "linux" ]; } && { [ "$XGOARCH" == "." ] || [ "$XGOARCH" == "amd64" ]; }; then
        GOOS=linux GOARCH=amd64 GO111MODULE=on CGO_ENABLED=0 go build -o "$LINUX_AMD64_DIR/tidba" -ldflags "$LDFLAGS" ${TiDBA}
    fi

    if { [ "$XGOOS" == "." ] || [ "$XGOOS" == "linux" ]; } && { [ "$XGOARCH" == "." ] || [ "$XGOARCH" == "arm64" ]; }; then
        GOOS=linux GOARCH=arm64 GO111MODULE=on CGO_ENABLED=0 go build -o "$LINUX_ARM64_DIR/tidba" -ldflags "$LDFLAGS" ${TiDBA}
    fi
done