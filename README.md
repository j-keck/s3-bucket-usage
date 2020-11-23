## get s3 bucket size

  - s3df: per CloudWatch API (fast)
  - s3du: loop over bucket content (can get the size from one folder in the bucket - SLOW)
  
I checked the results with the AWS tools for my test bucket:

  - s3df gives me 2.4GB, the AWS S3 Console gives me 2.3GB
  - s3du gives me 2.7GB, the AWS S3 cli gives me 2.7GB

## crosscheck per `aws s3`cli

_NOTE: update start / end time and bucket name!_
        	
    aws cloudwatch get-metric-statistics \
      --metric-name BucketSizeBytes \
      --namespace AWS/S3 \
      --start-time 2020-11-20T00:00:00Z \
      --end-time 2020-11-25T00:00:00Z \
      --period 3600 \
      --statistics Average \
      --unit Bytes \
      --dimensions Name=BucketName,Value=j-keck-testbucket Name=StorageType,Value=StandardStorage
        	 