# gompress
CLI tool to compress the contents of AWS S3 bucket.

Until better documentation is available, download the code, build it using Go
(1.13), and run the binary providing all the flags that the binary expects
(source and destination).

`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables must be
set, and they should belong to a user with sufficient permissions to list files
in s3 bucket, download them, and upload them to the location provided in
configuration.

Pre-compiled binaries, docker image and better docs are coming.
