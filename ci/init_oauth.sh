#!/bin/bash
set -e

wait_for_url() {
    URL=$1
    MSG=$2

    if [[ $URL == https* ]]; then
        CMD="curl -k -sL -o /dev/null -w %{http_code} $URL"
    else
        CMD="curl -sL -o /dev/null -w %{http_code} $URL"
    fi

    until [ "200" == "`$CMD`" ]
    do
        echo "$MSG ($URL)"
        sleep 2
    done
}

#URI=${HYDRA_URI}
#if [ "" == "${URI}" ]; then
#    URI="http://${HYDRA_HOST:-hydra}:{localhost:-4445}/clients"
#fi

wait_for_url "http://localhost:4445/clients" "Waiting for Hydra admin REST to start"


docker run --rm \
  --network hydra_default \
  oryd/hydra:v1.11.2 \
  clients create \
    --endpoint http://hydra:4445 \
    --id some-consumer \
    --secret some-secret \
    --grant-types client_credentials \
    --response-types token,code

docker run --rm \
  --network hydra_default \
  oryd/hydra:v1.11.2 \
  token client \
    --client-id some-consumer \
    --client-secret some-secret \
    --endpoint http://hydra:4444

curl http://localhost:4445/clients
