package awss3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

func awsAuthenticate(cfg *AWSS3Config) error {
	sess, err := getAWSSession(cfg)
	if err != nil {
		return err
	}

	cfg.Session = sess
	return nil
}

func getAWSSession(cfg *AWSS3Config) (*session.Session, error) {
	// If the access key and secret key are set, use them to create the session
	if cfg.Auth.AccessKey != "" && cfg.Auth.SecretKey != "" {
		return session.NewSession(&aws.Config{
			Region:      aws.String(cfg.Region),
			Credentials: credentials.NewStaticCredentials(cfg.Auth.AccessKey, cfg.Auth.SecretKey, ""),
		})
	}

	// If the profile is not set, use the default session
	if cfg.Profile == "" {
		return session.NewSession(&aws.Config{
			Region: aws.String(cfg.Region),
		})
	}

	// Otherwise if we have a profile set, create the session with this profile
	sess, err := session.NewSessionWithOptions(session.Options{
		Profile: cfg.Profile,
		Config: aws.Config{
			Region: aws.String(cfg.Region),
		},
	})

	return sess, err
}
