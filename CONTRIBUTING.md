Please refer to our [Contributions](https://github.com/mikependon/RepoDb#contributions) page for further information.

=== Building RepoDB ===

Call
     `$ dotnet build`

=== Test RepoDB ===

    `$ ./test-docker.ps1`
Requires docker, and diskspace and some memory to run all engines.

= Release RepoDb:

    `$ ./dotnet pack -c release -p Version=1.2.3 -o release`

This creates nuget packages for version 1.2.3 in the release directory

But it is better to just publish your changes on GitHub and make the GitHub
actions produce binaries in a reproducable way.
