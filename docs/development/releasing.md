# How To Release

## Release Branch

Always create a release branch for the releases. For example branch `release-0.5` is for all the `v0.5.x` version releases. If it's a new release branch, simply create a branch from `main`.

## Before Release 

If the new version to be released has backwards incompatible changes, i.e. it does not support older SDK versions, you 
must update the `pkg/sdkclient/serverinfo/versions.go` [file](https://github.com/numaproj/numaflow/blob/main/pkg/sdkclient/serverinfo/versions.go). Change the
value of the necessary languages to the new minimum supported SDK version. Ensure that this change is merged and included in the release.

## Release Steps

1. Cherry-pick fixes to the release branch, skip this step if it's the first release in the branch
2. Run `make test` to make sure all test cases pass locally
3. Push to remote branch, and make sure all the CI jobs pass
4. Run `make prepare-release VERSION=v{x.y.z}` to update version in manifests, where `x.y.z` is the expected new version
5. Follow the output of last step, to confirm if all the changes are expected, and then run `make release VERSION=v{x.y.z}`
6. Follow the output, push a new tag to the release branch, GitHub actions will automatically build and publish the new release, this will take around 10 minutes
7. Test the new release, make sure everything is running as expected, and then recreate a `stable` tag against the latest release
   ```shell
   git tag -d stable
   git tag -a stable -m stable
   git push -d {your-remote} stable
   git push {your-remote} stable
   ```
8. Find the new release tag, and edit the release notes
