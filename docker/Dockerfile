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

# First create a stage with just the Pulsar tarball and scripts
FROM busybox as kop

ARG PULSAR_VERSION=2.4.0

ADD target/kop-0.0.1-SNAPSHOT-bin.tar.gz /

RUN wget -q http://mirror.bit.edu.cn/apache/pulsar/pulsar-${PULSAR_VERSION}/apache-pulsar-${PULSAR_VERSION}-bin.tar.gz \
    && tar -zxf apache-pulsar-${PULSAR_VERSION}-bin.tar.gz \
    && mv apache-pulsar-${PULSAR_VERSION} /pulsar

RUN mv /kop-* /kop

COPY docker/scripts/apply-config-from-env.py  /kop/bin
COPY docker/scripts/run-broker.sh /kop/bin
COPY docker/scripts/run-standalone.sh /kop/bin
COPY docker/scripts/set_python_version.sh /kop/bin
COPY docker/scripts/watch-znode.py /kop/bin

### Create 2nd stage from OpenJDK image

FROM openjdk:8-jdk-slim
COPY --from=kop /kop /kop
COPY --from=kop /pulsar /pulsar

# Install some utilities
RUN apt-get update \
     && apt-get install -y netcat dnsutils \
                 python2.7 python-setuptools \
                 python3-setuptools \
                 libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev \
                 curl \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/*

# Install python3.5.6
RUN curl -O -L https://www.python.org/ftp/python/3.5.6/Python-3.5.6.tgz
RUN tar -zxvf Python-3.5.6.tgz
RUN cd Python-3.5.6 && ./configure --enable-optimizations && make altinstall

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
RUN python2.7 get-pip.py
RUN pip install kazoo pyyaml
RUN python3.5 get-pip.py

WORKDIR /kop

ENV PULSAR_ROOT_LOGGER=INFO,CONSOLE
