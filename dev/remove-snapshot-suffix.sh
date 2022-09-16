#!/usr/bin/env bash

set -eux -o pipefail

HOME="$(cd "$(dirname "$0")/.."; pwd)"

find "$HOME"/spark/v3.3/**/.out/libs/ -name "*.jar" -print0 | while read -d $'\0' file; do
  new_name=$(echo "$file" | sed -E  "s/-SNAPSHOT//")
  mv "$file" "$new_name"
done