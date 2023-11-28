package client

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/scratchdata/scratchdb/config"
)

type Client struct {
	S3  *s3.S3
	SQS *sqs.SQS
}

func NewClient(c *config.Config) *Client {
	awsCreds := credentials.NewStaticCredentials(c.AWS.AccessKeyId, c.AWS.SecretAccessKey, "")
	awsConfig := aws.NewConfig().
		WithRegion(c.AWS.Region).
		WithCredentials(awsCreds)

	if c.AWS.Endpoint != "" {
		awsConfig.WithEndpoint(c.AWS.Endpoint)
	}

	storageCreds := credentials.NewStaticCredentials(c.Storage.AccessKeyId, c.Storage.SecretAccessKey, "")
	storageConfig := aws.NewConfig().
		WithRegion(c.Storage.Region).
		WithCredentials(storageCreds).
		WithS3ForcePathStyle(true)

	if c.Storage.Endpoint != "" {
		storageConfig.WithEndpoint(c.Storage.Endpoint)
	}

	return &Client{
		S3:  s3.New(session.Must(session.NewSession()), storageConfig),
		SQS: sqs.New(session.Must(session.NewSession()), awsConfig),
	}
}
