package gompress_test

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/localstack"
	"github.com/orlangure/gompress/gompress"
	"github.com/stretchr/testify/require"
)

const (
	region       = "us-east-1"
	inputBucket  = "input-bucket"
	outputBucket = "output-bucket"
)

//nolint:gochecknoglobals
var (
	svc        *s3.S3
	s3Endpoint string
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	err := os.MkdirAll("./testdata/output-bucket", 0755)
	if err != nil {
		panic(err)
	}

	p := localstack.Preset(
		localstack.WithServices(localstack.S3),
		localstack.WithS3Files("./testdata"),
	)
	c, err := gnomock.Start(p)

	defer func() { _ = gnomock.Stop(c) }()

	if err != nil {
		panic(err)
	}

	s3Endpoint = fmt.Sprintf("http://%s/", c.Address(localstack.APIPort))
	config := &aws.Config{
		Region:           aws.String(region),
		Endpoint:         aws.String(s3Endpoint),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials("a", "b", "c"),
	}

	sess, err := session.NewSession(config)
	if err != nil {
		panic(err)
	}

	svc = s3.New(sess)

	return m.Run()
}

func TestGompress_removeOriginal(t *testing.T) {
	// start with 200 files
	listInput := &s3.ListObjectsV2Input{Bucket: aws.String(inputBucket)}
	files, err := svc.ListObjectsV2(listInput)
	require.NoError(t, err)
	require.Len(t, files.Contents, 200)

	conf := &gompress.Config{
		Src: &gompress.S3Locaction{
			Region: region,
			Bucket: inputBucket,
			Prefix: "a-",
		},
		Dst: &gompress.S3Locaction{
			Region: region,
			Bucket: outputBucket,
			Prefix: "new-dir/",
		},
		KeepOriginal: false, // remove original files from s3
		Endpoint:     s3Endpoint,
	}

	require.NoError(t, os.Setenv("AWS_ACCESS_KEY_ID", "foo"))
	require.NoError(t, os.Setenv("AWS_SECRET_ACCESS_KEY", "bar"))
	require.NoError(t, gompress.Run(conf))

	// should be 100 files now
	listInput = &s3.ListObjectsV2Input{Bucket: aws.String(inputBucket)}
	files, err = svc.ListObjectsV2(listInput)
	require.NoError(t, err)
	require.Len(t, files.Contents, 100)

	for _, f := range files.Contents {
		require.True(t, strings.HasPrefix(*f.Key, "b-"))
	}

	// should be 100 files in the target bucket as well
	listInput = &s3.ListObjectsV2Input{Bucket: aws.String(outputBucket)}
	files, err = svc.ListObjectsV2(listInput)
	require.NoError(t, err)
	require.Len(t, files.Contents, 100)

	for _, f := range files.Contents {
		base := path.Base(*f.Key)
		require.True(t, strings.HasSuffix(base, ".gz"))

		originalName := strings.TrimSuffix(base, ".gz")
		fName := path.Join("testdata", inputBucket, originalName)
		expectedBytes := readLocalFile(t, fName)

		actualBytes := readS3File(t, svc, outputBucket, *f.Key)

		require.Equal(t, expectedBytes, actualBytes)
	}
}

func TestGompress_keepOriginal(t *testing.T) {
	// we have 100 files left
	listInput := &s3.ListObjectsV2Input{Bucket: aws.String(inputBucket)}
	files, err := svc.ListObjectsV2(listInput)
	require.NoError(t, err)
	require.Len(t, files.Contents, 100)

	conf := &gompress.Config{
		Src: &gompress.S3Locaction{
			Region: region,
			Bucket: inputBucket,
			Prefix: "b-",
		},
		Dst: &gompress.S3Locaction{
			Region: region,
			Bucket: outputBucket,
			Prefix: "another-dir/",
		},
		KeepOriginal: true, // we now will keep the original files in s3
		Endpoint:     s3Endpoint,
	}

	require.NoError(t, os.Setenv("AWS_ACCESS_KEY_ID", "foo"))
	require.NoError(t, os.Setenv("AWS_SECRET_ACCESS_KEY", "bar"))
	require.NoError(t, gompress.Run(conf))

	// should still be 100 files
	listInput = &s3.ListObjectsV2Input{Bucket: aws.String(inputBucket)}
	files, err = svc.ListObjectsV2(listInput)
	require.NoError(t, err)
	require.Len(t, files.Contents, 100)

	for _, f := range files.Contents {
		require.True(t, strings.HasPrefix(*f.Key, "b-"))
	}

	// should be 100 files in the target path
	listInput = &s3.ListObjectsV2Input{
		Bucket: aws.String(outputBucket),
		Prefix: aws.String("another-dir/"),
	}
	files, err = svc.ListObjectsV2(listInput)
	require.NoError(t, err)
	require.Len(t, files.Contents, 100)

	for _, f := range files.Contents {
		base := path.Base(*f.Key)
		require.True(t, strings.HasSuffix(base, ".gz"))

		originalName := strings.TrimSuffix(base, ".gz")
		fName := path.Join("testdata", inputBucket, originalName)
		expectedBytes := readLocalFile(t, fName)

		actualBytes := readS3File(t, svc, outputBucket, *f.Key)

		require.Equal(t, expectedBytes, actualBytes)
	}
}

func readLocalFile(t *testing.T, file string) []byte {
	bs, err := ioutil.ReadFile(file) //nolint:gosec
	require.NoError(t, err)

	return bs
}

func readS3File(t *testing.T, svc *s3.S3, bucket, key string) []byte {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	out, err := svc.GetObject(input)
	require.NoError(t, err)

	defer func() { require.NoError(t, out.Body.Close()) }()

	gzipReader, err := gzip.NewReader(out.Body)
	require.NoError(t, err)

	defer func() { require.NoError(t, gzipReader.Close()) }()

	bs, err := ioutil.ReadAll(gzipReader)
	require.NoError(t, err)

	return bs
}
