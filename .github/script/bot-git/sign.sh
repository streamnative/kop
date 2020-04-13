#!/usr/bin/env bash

set -ex

DEST_PATH=$1
if [[ "x$DEST_PATH" == "x" ]]; then
  echo "You need give a file path to sign the file"
  exit 1
fi

pushd $DEST_PATH
echo "Sign files in path ${DEST_PATH}"
ls $DEST_PATH
FILES=`find . -type f | grep -v LICENSE | grep -v README`

export GPG_TTY=$(tty)
for FILE in $FILES
do
  echo "Signing $FILE"
  gpg --quiet --pinentry-mode loopback --yes --passphrase=${SNBOT_SECRET_PASSPHRASE} --armor --output $FILE.asc --detach-sig $FILE

  # SHA-512 signature
  shasum -a 512 $FILE > $FILE.sha512
done
echo "Signed done"
popd
