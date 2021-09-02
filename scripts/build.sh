#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -ex

echo "Releasing kop"

version=${1#v}
if [[ "x$version" == "x" ]]; then
  echo "You need give a version number of the kop"
  exit 1
fi

# Create a direcotry to save assets
ASSETS_DIR=release
mkdir $ASSETS_DIR

# Set to a new version
mvn versions:set -DnewVersion="${version}"

mvn clean install -DskipTests
cp kafka-impl/target/pulsar-protocol-handler-kafka-*.nar  ./$ASSETS_DIR
cp README.md ./$ASSETS_DIR/pulsar-protocol-handler-kafka-readme.md

OAUTH_CLIENT="./oauth-client/target/oauth-client-${version}.jar"
if [[ -f $OAUTH_CLIENT ]]; then
    cp "$OAUTH_CLIENT" "./$ASSETS_DIR"
fi
