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

set -e

if [ $# -eq 0 ]; then
    echo "Required argument with new pulsar version"
    exit 1
fi

NEW_VERSION=$1

# Go to top level project directory
pushd $(dirname "$0")/..

mvn versions:set-property -Dproperty=pulsar.version -DnewVersion=$NEW_VERSION

popd
