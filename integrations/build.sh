#!/usr/bin/env bash
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

BINDIR=$(dirname "$0")
INTR_HOME=`cd ${BINDIR}/..;pwd`

VERSION=$(${INTR_HOME}/scripts/dev/get-project-version.py)
TAG=${VERSION%"-SNAPSHOT"}
IMAGE_NAME_PREFIX="kop-test-"

build_image() {
    IMAGE=$1
    echo "Building test image : ${IMAGE}"
    docker build . -t ${IMAGE}
    echo "Successfully built test image : ${IMAGE}"
}

for img_dir in `ls -d ${INTR_HOME}/integrations/*/ | grep -v dev`; do
    BASE_NAME=$(basename ${img_dir})
    cd ${img_dir}
    if [[ $BASE_NAME == "kafka-client" ]]; then
        VERSIONS=(1.0.0 1.1.0 2.0.0 2.1.0 2.2.0 2.3.0 2.4.0 2.5.0 2.6.0)
        for VERSION in ${VERSIONS[@]}; do
            sed -i '' "s/<kafka\.version>.*<\/kafka\.version>/<kafka.version>$VERSION<\/kafka.version>/" pom.xml
            build_image "streamnative/${IMAGE_NAME_PREFIX}${BASE_NAME}-${VERSION}:${TAG}"
            sed -i '' "s/<kafka\.version>.*<\/kafka\.version>/<kafka.version>2.6.0<\/kafka.version>/" pom.xml
        done
    else
        build_image "streamnative/${IMAGE_NAME_PREFIX}${BASE_NAME}:${TAG}"
    fi
done
