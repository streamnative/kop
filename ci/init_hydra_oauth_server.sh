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

wait_for_url() {
    URL=$1
    MSG=$2

    if [[ $URL == https* ]]; then
        CMD="curl -k -sL -o /dev/null -w %{http_code} $URL"
    else
        CMD="curl -sL -o /dev/null -w %{http_code} $URL"
    fi

    until [ "200" == "$($CMD)" ]
    do
        echo "$MSG ($URL)"
        sleep 2
    done
}

# Start hydra server
docker-compose -f ci/hydra/docker-compose.yml up -d

# Wait until the hydra server started
wait_for_url "http://localhost:4445/clients" "Waiting for Hydra admin REST to start"

# Delete access token
docker run --rm \
  --network hydra_default \
  oryd/hydra:v1.11.7 \
  --endpoint=http://hydra:4445 \
  keys delete hydra.jwt.access-token

# Replace access token
docker run --rm \
  --network hydra_default \
  -v "${PWD}"/ci/hydra/keys:/tmp/keys \
  oryd/hydra:v1.11.7 \
  --endpoint=http://hydra:4445 \
  keys import hydra.jwt.access-token /tmp/keys/private_key.json /tmp/keys/public_key.json

# Create a new client
docker run --rm \
  --network hydra_default \
  oryd/hydra:v1.11.7 \
  clients create \
    --endpoint http://hydra:4445 \
    --id simple_client_id \
    --secret simple_client_secret \
    --grant-types client_credentials \
    --response-types token,code \
    --token-endpoint-auth-method client_secret_post \
    --audience http://example.com/api/v2/

# Try to generate a token using the new client
docker run --rm \
  --network hydra_default \
  oryd/hydra:v1.11.7 \
  token client \
    --client-id simple_client_id \
    --client-secret simple_client_secret \
    --endpoint http://hydra:4444

