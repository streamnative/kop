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
3. Update the project version and tag
4. Build the artifacts
5. Verify the artifacts
6. Move master branch to the next version
7. Write release notes

## Steps in detail

1. Prepare for a release

   Create a new milestone and move the pull requests that can not  be published in this release to the new milestone.

2. Create the release branch

   ```bash
   $ git clone git@github.com:streamnative/kop.git
   $ cd kop
   $ git checkout -b branch-X.Y.Z
   ```

3. Update the project version and tag

   > If the dependency of Apache Pulsar needs to change, i.e. the project version was from `X1.Y1.Z1.M` to `X2.Y2.Z2.0`, you should change the dependency of Apache Pulsar first.

   ```bash
   $ ./scripts/set-project-version.sh X.Y.Z.M
   $ git commit -m "Release X.Y.Z.M" -a
   $ git push origin branch-X.Y.Z
   $ git tag vX.Y.Z.M
   $ git push origin vX.Y.Z.M
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

6. Move master branch to the next version

   ```bash
   $ git checkout master
   $ ./scripts/set-project-version.sh X.Y.Z-SNAPSHOT
   $ git commit -m 'Bumped version to X.Y.Z-SNAPSHOT' -a
   ```

7. Write release notes

   Release notes is mainly to track the document catch up work.

   You should document the following things at your release notes:

   - Feature
   - Bug fixed
