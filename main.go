package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"strings"
)

func main() {
	awsAccessKeyId := "xxxx"
	awsSecretAccessKey := "yyyy"
	bucket := "j-keck-testbucket"
	prefix := ""
	region := "eu-central-1"

	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(
			awsAccessKeyId,
			awsSecretAccessKey,
			""),
		Region: aws.String(region),
	})
	if err != nil {

	}

	s3Sess := s3.New(sess)
	size, err := s3df(s3Sess, bucket, prefix)

	fmt.Printf("bucket: %s, prefix: %s - size: %d bytes\n", bucket, prefix, size)
}

func s3df(s3sess *s3.S3, bucket string, prefix string) (int64, error) {

	// if it has a '/' prefix, remove it
	if strings.HasPrefix(prefix, "/") {
		prefix = prefix[1:]
	}

	var size int64
	var fetcher func(*string) error
	fetcher = func(continuationToken *string) error {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			MaxKeys:           aws.Int64(1000), // aws s3client default value
			ContinuationToken: continuationToken,
		}

		res, err := s3sess.ListObjectsV2(input)
		if err != nil {
			return errors.Wrap(err, "token: "+*continuationToken)
		}

		for _, obj := range res.Contents {
			size += *obj.Size
		}

		// fetch next range
		if *res.IsTruncated {
			return fetcher(res.NextContinuationToken)
		}

		return nil
	}

	if err := fetcher(nil); err != nil {
		return -1, errors.Wrap(err, "s3df: fetcher")
	}

	return size, nil
}
