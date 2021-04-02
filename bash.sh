USER=congbobo184 # TODO: change it to your github id
BASE_BRANCH=branch-2.7.0 # TODO: change it to the real branch that should merge your PR
PULSAR_VERSION=2.7.1.2-rc-202104012016 # TODO: change it to the real rc version of your pulsar
PROJECT=$1
# 1. checkout to `BASE_BRANCH`. You mush make sure that `BASE_BRANCH` existed.
BRANCH="$USER/bump-$PULSAR_VERSION"
git checkout -b "$BRANCH"
# 2. Update pulsar version and project version
./scripts/set-pulsar-version.sh $PULSAR_VERSION
./scripts/set-project-version.sh $PULSAR_VERSION
# 3. Commit the update and push the commit
git add .
git commit -m "Bump pulsar to $PULSAR_VERSION"
git push origin "$BRANCH"
