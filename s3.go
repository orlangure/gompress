package main

import (
	"fmt"
	"io"
	"log"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func newClient(region, bucket, prefix string) (*client, error) {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return nil, fmt.Errorf("can't create session: %w", err)
	}

	return &client{sess, s3.New(sess), bucket, prefix}, nil
}

type client struct {
	session *session.Session
	s3      *s3.S3
	bucket  string
	prefix  string
}

func (c *client) listFiles() (<-chan string, <-chan error) {
	files := make(chan string)
	errors := make(chan error, 1)

	go func() {
		defer func() {
			close(files)
			close(errors)
		}()

		var continuationToken *string

		moreFilesAvailable := true
		for moreFilesAvailable {
			listInput := &s3.ListObjectsV2Input{
				Bucket: aws.String(c.bucket),
				Prefix: aws.String(c.prefix),
			}

			if continuationToken != nil {
				listInput.ContinuationToken = continuationToken
			}

			output, err := c.s3.ListObjectsV2(listInput)
			if err != nil {
				errors <- fmt.Errorf("can't list objects: %w", err)
				return
			}

			moreFilesAvailable = *output.IsTruncated
			continuationToken = output.NextContinuationToken

			log.Printf(
				"got %d files, more available: %v\n",
				len(output.Contents), moreFilesAvailable,
			)

			for _, file := range output.Contents {
				files <- *file.Key
			}
		}
	}()

	return files, errors
}

func (c *client) read(file string) (io.ReadCloser, error) {
	getObjInput := &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(file),
	}

	output, err := c.s3.GetObject(getObjInput)
	if err != nil {
		return nil, fmt.Errorf("can't get object: %w", err)
	}

	return output.Body, nil
}

func (c *client) write(file string, r io.Reader) error {
	fName := path.Base(file)
	fName = path.Join(c.prefix, fName) + ".gz"
	uploader := s3manager.NewUploader(c.session)

	_, err := uploader.Upload(&s3manager.UploadInput{
		Body:   r,
		Bucket: aws.String(c.bucket),
		Key:    aws.String(fName),
	})
	if err != nil {
		return fmt.Errorf("can't upload file %s: %w", fName, err)
	}

	return nil
}

func (c *client) delete(file string) error {
	_, err := c.s3.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(file),
	})
	if err != nil {
		return fmt.Errorf("can't delete object %s: %w", file, err)
	}

	return nil
}
