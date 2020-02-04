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

##  TODO: change this home dir, since moved get-project-version.py
##    from root/dev into integrations/dev
echo "++++ dir Bindir : ${BINDIR}, INTR_HOME: ${INTR_HOME}"

VERSION=$(${INTR_HOME}/dev/get-project-version.py)
TAG=${VERSION%"-SNAPSHOT"}
IMAGE_NAME_PREFIX="kop-test-"

if [ -z "$DOCKER_USER" ]; then
    echo "Docker user in variable \$DOCKER_USER was not set. Skipping image publishing"
    exit 1
fi

if [ -z "$DOCKER_PASSWORD" ]; then
    echo "Docker password in variable \$DOCKER_PASSWORD was not set. Skipping image publishing"
    exit 1
fi

docker login ${DOCKER_REGISTRY} -u="${DOCKER_USER}" -p="${DOCKER_PASSWORD}"
if [ $? -ne 0 ]; then
    echo "Failed to loging to Docker Hub ${DOCKER_REGISTRY}"
    exit 1
fi

for img_dir in `ls -d ${INTR_HOME}/integrations/*/`; do
    BASE_NAME=$(basename ${img_dir})
    IMAGE="streamnative/${IMAGE_NAME_PREFIX}${BASE_NAME}:${TAG}"
    IMAGE_LATEST="streamnative/${IMAGE_NAME_PREFIX}${BASE_NAME}:latest"
    docker tag ${IMAGE} ${IMAGE_LATEST}
    docker push ${IMAGE_LATEST}
    docker push ${IMAGE}
done
