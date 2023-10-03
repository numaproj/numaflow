# How To Release

## Release Branch

Always create a release branch for the releases, for example branch `release-0.5` is for all the v0.5.x versions release. If it's a new release branch, simply create a branch from `main`.

## Release Steps

1. Cherry-pick fixes to the release branch, skip this step if it's the first release in the branch.
1. Run `make test` to make sure all test cases pass locally.
1. Push to remote branch, and make sure all the CI jobs pass.
1. Run `make prepare-release VERSION=v{x.y.z}` to update version in manifests, where `x.y.x` is the expected new version.
1. Follow the output of last step, to confirm if all the changes are expected, and then run `make release VERSION=v{x.y.z}`.
1. Follow the output, push a new tag to the release branch, GitHub actions will automatically build and publish the new release, this will take around 10 minutes.
1. Test the new release, make sure everything is running as expected, and then recreate a `stable` tag against the latest release.
   ```shell
   git tag -d stable
   git tag -a stable -m stable
   git push -d {your-remote} stable
   git push {your-remote} stable
   ```
1. Find the new release tag, and edit the release notes.
