# Release process

This guide illustrates how to perform a release for kop.

## Naming convention

All StreamNative repository should following the naming convention:

- Branch name: `branch-X.Y.Z`
- Tag name: `vX.Y.Z(.M)`(stable)
- Release candidate tag: `vX.Y.Z(.M)-rc-$(date +%Y%m%d%H%M)` (unstable)

`(.M)` means our internal version release number, most of our repository is an extensions/tools for apache/pulsar. To keep track of the repository is produced by which version of the apache/pulsar, we will carry the apache/pulsar version number and using the `.M` to represent our internal release version. And all repository should keep align with streamnative/pulsar.

There has two type of the tags, one is stable `vX.Y.Z(.M)`, and another is unstable `vX.Y.Z(.M)-rc-$(date +%y%m%d%H%M)`. A stable tag represent this release is a verified release, and an unstable tag represent this release is not verified.

## Release workflow

1. Prepare for a release
2. Create the release branch
3. Update the project version and pulsar version
4. Build the artifacts
5. Verify the artifacts
6. Release the artifacts using streamnative-ci
7. Write release notes
8. Move master branch to the next version

## Steps in detail

> In this section, `X.Y.Z.M` represents a specific version, e.g. `2.6.2.0`.

1. Prepare for a release

   Create a new milestone and move the pull requests that can not be published in this release to the new milestone.

2. Create the release branch

   ```bash
   $ git clone git@github.com:streamnative/kop.git
   $ cd kop
   $ git checkout -b bump-pulsar-version
   ```

3. Update the project version and pulsar version

   ```bash
   $ ./scripts/set-project-version.sh X.Y.Z.M
   $ ./scripts/set-pulsar-version.sh X.Y.Z.M
   $ git commit -m "Release X.Y.Z.M" -a
   $ git push origin bump-pulsar-version
   ```

4. Build the artifacts

   ```bash
   $ mvn clean install -DskipTests
   ```

5. Verify the artifacts

   You can run the following commands to verify it in local environment:

   ```bash
   $ mvn checkstyle:check
   $ mvn spotbugs:check
   $ mvn test -DfailIfNoTests=false '-Dtest=!KafkaIntegrationTest,!DistributedClusterTest'
   $ mvn test '-Dtest=KafkaIntegrationTest' -pl tests
   ```

   See [pr-test.yml](.github/workflows/pr-test.yml) for details.

   Then you should push a PR to merge `branch-X.Y.Z` to `master` for running CI tests using Github Actions.

   If the verification failed, which may be caused by the incompatibility with new Pulsar version, we need to add more commits to fix it.

6. Release the artifacts using streamnative-ci

   After the PR being merged to `master`, you can use streamnative-ci to release the artifacts.

   The streamnative-ci needs the specified tag, so we need to push a tag `vX.Y.Z.M` first, as well as the branch:

   ```bash
   $ git clone git@github.com:streamnative/kop.git
   $ cd kop
   $ git checkout -b branch-X.Y.Z
   $ git push origin branch-X.Y.Z
   $ git tag vX.Y.Z.M
   $ git push origin vX.Y.Z.M
   ```

   Then use streamnative-ci to release the latest KoP:

   ```bash
   $ git clone git@github.com:streamnative/streamnative-ci.git
   $ cd streamnative-ci
   $ git checkout release
   $ git commit --allow-empty -m "/snbot release kop vX.Y.Z.M"
   $ git push origin release
   # Then, you can see the release process: https://github.com/streamnative/streamnative-ci/actions
   ```

7. Write release notes

   Release notes is mainly to track the document catch up work.

   You should document the following things at your release notes:

   - Feature
   - Bug fixed

8. Move master branch to the next version

   ```bash
   $ git checkout -b bump-master
   $ ./scripts/set-project-version.sh X.Y.Z-SNAPSHOT
   $ git commit -m 'Bump version to X.Y.Z-SNAPSHOT' -a
   $ git push origin bump-master
   # create a PR for this change
   ```
