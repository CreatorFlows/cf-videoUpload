package aws

import (
	"context"
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var AWS_CONFIG struct {
	ACCESS_KEY string
	SECRET_KEY string
	REGION     string
}

var (
	AWS_CONFIG_V2 aws.Config
	S3_CLIENT     *s3.Client
	once          sync.Once
)

func AWSClient() error {
	var err error
	once.Do(func() {
		// Load AWS configuration
		AWS_CONFIG_V2, err = config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(AWS_CONFIG.REGION),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				AWS_CONFIG.ACCESS_KEY, AWS_CONFIG.SECRET_KEY, "")),
		)
		if err != nil {
			err = errors.New("unable to load AWS SDK config")
			return
		}
	})
	return err
}

func S3Client() error {
	var err error
	once.Do(func() {
		S3_CLIENT = s3.NewFromConfig(AWS_CONFIG_V2)
	})
	if S3_CLIENT == nil {
		err = errors.New("S3 Client not initialized")
	}
	return err
}
