// Package gompress exposes a function to compress the contents of an S3 bucket
package gompress

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"sync"
)

// Run starts gompress with the provided configuration
func Run(conf *Config) error {
	src, err := newSrcClient(conf)
	if err != nil {
		return fmt.Errorf("can't create source s3 client: %w", err)
	}

	dst, err := newDstClient(conf)
	if err != nil {
		return fmt.Errorf("can't create destination s3 client: %w", err)
	}

	files, errors := src.listFiles()

	wg := &sync.WaitGroup{}
	w := &worker{src, dst, conf.KeepOriginal}

	for i := 0; i < 4; i++ {
		wg.Add(1)

		go w.start(files, wg)
	}

	wg.Wait()

	err = <-errors
	if err != nil {
		return fmt.Errorf("finished with error: %w", err)
	}

	return nil
}

// Config defines how gompress will process the files
type Config struct {
	// Src defines where to take the data from
	Src *S3Locaction

	// Dst defines where to put compressed data
	Dst *S3Locaction

	// KeepOriginal is a flag that allows to keep or remove compressed files in
	// Src location
	KeepOriginal bool

	// Endpoint is used for tests to override default s3 endpoint
	Endpoint string
}

// S3Locaction defines a path in s3, including region, bucket and prefix
type S3Locaction struct {
	// Region is AWS region
	Region string

	// Bucket is a bucket in s3
	Bucket string

	// Prefix is a path inside the bucket, or part of it
	Prefix string
}

type worker struct {
	src *client
	dst *client

	keep bool
}

func (w *worker) start(filesChan <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	for file := range filesChan {
		err := copyCompressedFile(w.src, w.dst, file)
		if err != nil {
			log.Println("can't move compressed file", file, "error:", err)
			continue
		}

		log.Println("copied", file)

		if w.keep {
			continue
		}

		// delete file
		err = w.src.delete(file)
		if err != nil {
			log.Println("can't delete source file", file, "error:", err)
			continue
		}

		log.Println("removed file", file)
	}
}

func copyCompressedFile(src, dst *client, file string) error {
	srcReader, err := src.open(file)
	if err != nil {
		return fmt.Errorf("can't read source file %s: %w", file, err)
	}

	uploadReader, pipeWriter := io.Pipe()
	gzipWriter := gzip.NewWriter(pipeWriter)

	wg := sync.WaitGroup{}

	wg.Add(1)

	errors := make(chan error, 6) // 6 potential errors below

	go func(file string) {
		defer func() {
			closeResource(srcReader, errors)
			closeResource(gzipWriter, errors)
			closeResource(pipeWriter, errors)
			wg.Done()
		}()

		_, err := io.Copy(gzipWriter, srcReader)
		if err != nil {
			errors <- fmt.Errorf("can't copy file %s: %w", file, err)
		}
	}(file)

	err = dst.write(file, uploadReader)
	if err != nil {
		errors <- fmt.Errorf("can't write to destination file %s: %w", file, err)
	}

	err = uploadReader.Close()
	if err != nil {
		errors <- fmt.Errorf("can't close upload reader for file %s: %w", file, err)
	}

	wg.Wait()
	closeResource(uploadReader, errors)
	close(errors)

	return consumeErrors(errors)
}

func closeResource(c io.Closer, errors chan<- error) {
	err := c.Close()
	if err != nil {
		errors <- fmt.Errorf("can't close resource: %w", err)
	}
}

func consumeErrors(errors <-chan error) error {
	var err error

	for err = range errors {
		log.Println(err)
	}

	return err
}
