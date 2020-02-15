package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

func main() {
	conf, err := newConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %s", err)
	}

	src, err := newClient(conf.srcRegion, conf.srcBucket, conf.srcPrefix)
	if err != nil {
		log.Fatalf("can't create source s3 client: %s", err)
	}

	dst, err := newClient(conf.dstRegion, conf.dstBucket, conf.dstPrefix)
	if err != nil {
		log.Fatalf("can't create destination s3 client: %s", err)
	}

	files, errors := src.listFiles()

	wg := &sync.WaitGroup{}
	w := &worker{src, dst, conf.keepOriginal}

	for i := 0; i < 4; i++ {
		wg.Add(1)

		go w.start(files, wg)
	}

	wg.Wait()

	err = <-errors
	if err != nil {
		log.Println("finished with error:", err)
		os.Exit(1)
	}

	log.Println("finished successfully")
}

type config struct {
	srcRegion string
	srcBucket string
	srcPrefix string

	dstRegion string
	dstBucket string
	dstPrefix string

	keepOriginal bool
}

func newConfig() (*config, error) {
	var srcRegion, srcBucket, srcPrefix string

	flag.StringVar(&srcRegion, "src-region", "us-east-1", "source region")
	flag.StringVar(&srcBucket, "src-bucket", "", "source s3 bucket name")
	flag.StringVar(&srcPrefix, "src-prefix", "", "source file prefix")

	var dstRegion, dstBucket, dstPrefix string

	flag.StringVar(&dstRegion, "dst-region", "us-east-1", "target region")
	flag.StringVar(&dstBucket, "dst-bucket", "", "target s3 bucket name")
	flag.StringVar(&dstPrefix, "dst-prefix", "", "new files will be prefixed with this value")

	keepOriginal := flag.Bool("keep", false, "set to keep original files (remove by default)")

	flag.Parse()

	if srcRegion == "" {
		return nil, fmt.Errorf("invalid source region '%s'", srcRegion)
	}

	if srcBucket == "" {
		return nil, fmt.Errorf("invalid source bucket '%s'", srcBucket)
	}

	if dstRegion == "" {
		return nil, fmt.Errorf("invalid destination region '%s'", dstRegion)
	}

	if dstBucket == "" {
		return nil, fmt.Errorf("invalid destination bucket '%s'", dstBucket)
	}

	return &config{
		srcRegion, srcBucket, srcPrefix,
		dstRegion, dstBucket, dstPrefix,
		*keepOriginal,
	}, nil
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

		if w.keep {
			continue
		}

		// delete file
		err = w.src.delete(file)
		if err != nil {
			log.Println("can't delete source file", file, "error:", err)
			continue
		}

		log.Println("moved file", file)
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
