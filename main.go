package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"strings"
	"time"
)

func main() {
	awsAccessKeyId := "xxxx"
	awsSecretAccessKey := "yyyy"
	bucket := "j-keck-testbucket"
	region := "eu-central-1"

	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(
			awsAccessKeyId,
			awsSecretAccessKey,
			""),
		Region: aws.String(region),
	})
	/*
	    // use credentials from `$HOME/.aws/credentials`
		sess := session.Must(session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		}))
	*/
	if err != nil {
		panic(err)
	}

	/*
		{
			prefix := ""

			// !! SLOW !!
			size, err := s3du(sess, bucket, prefix)
			if err != nil {
				panic(err)
			}
			fmt.Printf("bucket: %s, prefix: %s - size: %d bytes, %s\n", bucket, prefix, size, bytes2human(size))
		}
	*/

	size, err := s3df(sess, bucket)
	if err != nil {
		panic(err)
	}
	fmt.Printf("bucket: %s, size: %d bytes, %s\n", bucket, size, bytes2human(size))
}

// df is fast (precalculated values from CloudWatch)
func s3df(sess *session.Session, bucket string) (int64, error) {
	svc := cloudwatch.New(sess)

	//
	// build the query

	// https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricDataQuery.html
	// https://docs.aws.amazon.com/sdk-for-go/api/service/cloudwatch/#MetricDataQuery
	query := &cloudwatch.MetricDataQuery{
		Id: aws.String("bucketSize"),
		MetricStat: &cloudwatch.MetricStat{
			Stat:   aws.String("Maximum"),
			Period: aws.Int64(300),
			Metric: &cloudwatch.Metric{
				Namespace:  aws.String("AWS/S3"),
				MetricName: aws.String("BucketSizeBytes"),
				Dimensions: []*cloudwatch.Dimension{
					&cloudwatch.Dimension{
						Name:  aws.String("BucketName"),
						Value: aws.String(bucket),
					},
					&cloudwatch.Dimension{
						Name:  aws.String("StorageType"),
						Value: aws.String("StandardStorage"),
					},
				},
			},
		},
	}

	//
	// execute the query

	// we look n days back so we get a metric even if some days are missing
	durStart, _ := time.ParseDuration("-72h")
	data, err := svc.GetMetricData(&cloudwatch.GetMetricDataInput{
		StartTime:         aws.Time(time.Now().Add(durStart)),
		EndTime:           aws.Time(time.Now()),
		MetricDataQueries: []*cloudwatch.MetricDataQuery{query},
	})
	if err != nil {
		return -1, errors.Wrap(err, "s3df: GetMetricData")
	}

	//
	// get the result

	var maxSize int64
	for _, metricdata := range data.MetricDataResults {
		switch *metricdata.Id {
		case "bucketSize":

			for idx, _ := range metricdata.Timestamps {
				// DEBUG LOG (where is zip?)
				// fmt.Printf("%v %v\n", (*metricdata.Timestamps[idx]).String(), bytes2human(int64(*metricdata.Values[idx])))

				size := int64(*metricdata.Values[idx])
				if maxSize < size {
					maxSize = size
				}
			}

		default:
			return -1, fmt.Errorf("Unexpected metric-data-result with id: %s", *metricdata.Id)
		}
	}

	return maxSize, nil
}

// du is slow but it can give you the size of one folder in a bucket
func s3du(sess *session.Session, bucket string, prefix string) (int64, error) {
	s3sess := s3.New(sess)

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
		return -1, errors.Wrap(err, "s3du: fetcher")
	}

	return size, nil
}

// from https://yourbasic.org/golang/formatting-byte-size-to-human-readable-format/
func bytes2human(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}
