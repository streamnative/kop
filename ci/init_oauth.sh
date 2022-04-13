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

URI=${HYDRA_URI}
if [ "" == "${URI}" ]; then
    URI="http://${HYDRA_HOST:-hydra}:{localhost:-4445}/clients"
fi

wait_for_url $URI "Waiting for Hydra admin REST to start"



