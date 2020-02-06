# Release process

This guide illustrates how to perform a release for kop.

## Making the release

The steps for releasing are as follows:

1. Prepare for a release
2. Create the release branch
3. Update project version and tag
4. Write a release note
5. Move master branch to next version
6. Announce the release

## Steps in detail

1. Prepare for a release

Create a new milestone and move the pull requests that can not
be published in this release to the new milestone.

2. Create the release branch

We are going to create a branch from `master` to `branch-vx.y.z`
where the tag will be generated and where new fixes will be
applied as part of the maintenance for the release. `x.y.z`
is the version of the release.

The branch needs only to be created when creating major releases,
and not for patch releases.

Eg: When creating `v0.1.1` release, will be creating
the branch `branch-0.1.1`, but for `v0.1.2` we
would keep using the old `branch-0.1.1`.

In these instructions, I'm referring to an fictitious release `x.y.z`.
Change the release version in the examples accordingly with the real version.

It is recommended to create a fresh clone of the repository to 
avoid any local files to interfere in the process:

```shell
$ git clone git@github.com:streamnative/kop.git
$ cd kop

# Create a branch
$ git checkout -b branch-x.y.z origin/master

# Create a tag
$ git tag -u $USER@streamnative.io vx.y.z -m 'Release vx.y.z'
```

3. Update project version and tag

In this process the maven version of the project will always be the final one.

```bash
# Bump to the release version
$ ./secrets/set-project-version.sh x.y.z

# Commit
$ git commit -m 'Release x.y.z' -a

# Push both the branch and the tag to Github repo
$ git push origin branch-x.y.z
$ git push origin vx.y.z
```

4. Publish a release note

Check the milestone in GitHub associated with the release. 

In the released item, add the list of the most important changes 
that happened in the release and a link to the associated milestone,
with the complete list of all the changes. 

Update the release draft at [the release homepage of kop](https://github.com/streamnative/kop/releases)

Then the release draft, binary, and command doc will be published
 o that release automatically.

5. Move master branch to next version

We need to move master version to next iteration `X + 1`.

```bash
$ git checkout master
$ ./secrets/set-project-version.sh 0.Y.0-SNAPSHOT

$ git commit -m 'Bumped version to 0.Y.0-SNAPSHOT' -a
```

6. Announce the release

After publishing and checking the release, work with Growth team
to announce that a new version of kop is released.
