package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/orlangure/gompress/gompress"
)

func main() {
	conf, err := newConfig()
	if err != nil {
		log.Fatalf("can't configure gompress: %s", err.Error())
	}

	err = gompress.Run(conf)
	if err != nil {
		log.Fatalf("error in gompress: %s", err.Error())
	}

	log.Println("completed successfully")
}

func newConfig() (*gompress.Config, error) {
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

	return &gompress.Config{
		Src: &gompress.S3Locaction{
			Region: srcRegion,
			Bucket: srcBucket,
			Prefix: srcPrefix,
		},
		Dst: &gompress.S3Locaction{
			Region: dstRegion,
			Bucket: dstBucket,
			Prefix: dstPrefix,
		},
		KeepOriginal: *keepOriginal,
	}, nil
}
