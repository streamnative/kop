#!/usr/bin/env bash

set -ex

BINDIR=`dirname "$0"`
CI_SCRIPTS_HOME=`cd ${BINDIR};pwd`

RM_USER=${RM_USER:-"snbot"}
CAPITALIZED_RM_USER=`echo "${RM_USER}" | tr '[a-z]' '[A-Z]'`

# Decrypt the file
#mkdir -p ${CI_SCRIPTS_HOME}/gpg

PASSVAR="${CAPITALIZED_RM_USER}_SECRET_PASSPHRASE"

# --batch to prevent interactive command --yes to assume "yes" for questions
gpg --quiet --batch --yes --decrypt --passphrase="${!PASSVAR}" \
    --output ${CI_SCRIPTS_HOME}/private-key.asc.gpg ${CI_SCRIPTS_HOME}/${RM_USER}/private-key.asc.gpg

# import the gpg key
gpg --import --batch ${CI_SCRIPTS_HOME}/private-key.asc.gpg
