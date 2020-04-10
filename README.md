# gompress
CLI tool to compress the contents of AWS S3 bucket.

Gompress takes files from the original location, compresses them on the fly
using gzip, and uploads to their destination, keeping the original names (only
adding `.gz`).

Gompress never keeps large files completely in memory, and never writes them to
disk. It streams data from source into destination, adding a compression layer
in the middle.

It is recommended to become familiar with [AWS S3
pricing](https://aws.amazon.com/s3/pricing/) before using this utility. Moving
files around costs money. To reduce the cost, use EC2 instances in the same
region as the buckets!

## Usage

### Running in a docker container:

```
docker run --rm \
    -e AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY \
    orlangure/gompress \
    -src-region us-east-1 \
    -src-bucket uncompressed-files \
    -src-prefix some-folder/and-sub-folder \
    -dst-region us-east-1 \
    -dst-bucket compressed-files \
    -dst-prefix a-folder \
    -keep
```

AWS credentials with sufficient permissions to list, get, create and delete
objects are required. AWS key and secret should be provided using [environment
variables](https://docs.docker.com/engine/reference/commandline/run/#set-environment-variables--e---env---env-file).

When `-keep` flag is provided, original files remain in their original
location. Otherwise, when `-keep` flag is not set, original files are removed
once their compressed versions are successfully uploaded to the destination
path.

### Building from source (Go 1.13 or above is required):

```
$ git clone https://github.com/orlangure/gompress
$ cd gompress
$ go install .

$ gompress
Usage of gompress:
  -dst-bucket string
        target s3 bucket name
  -dst-prefix string
        new files will be prefixed with this value
  -dst-region string
        target region (default "us-east-1")
  -keep
        set to keep original files (remove by default)
  -src-bucket string
        source s3 bucket name
  -src-prefix string
        source file prefix
  -src-region string
        source region (default "us-east-1")
```
