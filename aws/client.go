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

type AWS_CONFIG struct {
	ACCESS_KEY string
	SECRET_KEY string
	REGION     string
}

var (
	AWS_CONFIG_V2 aws.Config
	S3_CLIENT     *s3.Client
	once          sync.Once
)

func AWSClient(cfg AWS_CONFIG) error {
	var err error
	once.Do(func() {
		AWS_CONFIG_V2, err = config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(cfg.REGION),
			config.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(
					cfg.ACCESS_KEY,
					cfg.SECRET_KEY,
					"",
				)),
		)
		if err != nil {
			err = errors.New("unable to load AWS SDK config")
			return
		}
	})
	return err
}

func S3Client() error {
	S3_CLIENT = s3.NewFromConfig(AWS_CONFIG_V2)
	if S3_CLIENT == nil {
		return errors.New("S3 Client not initialized")
	}
	return nil
}
