package awss3

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func awsAuthenticate(walCfg *AWSS3Config) error {
	awsConfig, err := getAWSConfig(walCfg)
	if err != nil {
		return err
	}

	walCfg.AWSConfig = awsConfig
	return nil
}

func getAWSConfig(walCfg *AWSS3Config) (*aws.Config, error) {
	var cfg aws.Config
	var err error

	// If the access key and secret key are set, use them to create the config
	if walCfg.Auth.AccessKey != "" && walCfg.Auth.SecretKey != "" {
		creds := aws.Credentials{
			AccessKeyID:     walCfg.Auth.AccessKey,
			SecretAccessKey: walCfg.Auth.SecretKey,
		}

		if cfg, err = config.LoadDefaultConfig(context.TODO(), config.WithCredentialsProvider(credentials.StaticCredentialsProvider{Value: creds})); err != nil {
			return nil, err
		}
		return &cfg, nil
	}

	// If the profile is not set, use the default config
	if walCfg.Profile == "" {
		if cfg, err = config.LoadDefaultConfig(context.TODO(), config.WithRegion(walCfg.Region)); err != nil {
			return nil, err
		}
		return &cfg, nil
	}

	// Otherwise if we have a profile set, create the session with this profile
	if cfg, err = config.LoadDefaultConfig(context.TODO(), config.WithSharedConfigProfile(walCfg.Profile), config.WithRegion(walCfg.Region)); err != nil {
		return nil, err
	}
	return &cfg, nil
}
