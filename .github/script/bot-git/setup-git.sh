#!/usr/bin/env bash

set -ex

BINDIR=`dirname "$0"`
CI_SCRIPTS_HOME=`cd ${BINDIR};pwd`

RM_USER=${RM_USER:-"snbot"}

# setup git
cp ${CI_SCRIPTS_HOME}/${RM_USER}/.gitconfig ${HOME}/.gitconfig

# import keys
${CI_SCRIPTS_HOME}/import-gpg-key.sh
