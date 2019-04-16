#!/usr/bin/env bash

set -o nounset

VERSION=$1
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
FILE="$DIR/setup.py"
sed -i -e "s/version='dev'/version='$VERSION'/g" $FILE
echo "updated $FILE file with version $VERSION"