//go:build integration

package s3store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/spectremi/open-aspm/internal/blobstore"
	"github.com/spectremi/open-aspm/internal/blobstore/blobstoretest"
)

func TestS3Conformance(t *testing.T) {
	client, bucket := newTestBucket(t)
	sequence := 0
	blobstoretest.Run(t, func(t *testing.T) blobstore.Store {
		t.Helper()
		sequence++
		store, err := New(client, Config{
			Bucket: bucket,
			Prefix: "conformance/" + strconv.Itoa(sequence),
		})
		if err != nil {
			t.Fatal(err)
		}
		return store
	})
}

func TestS3OpenDetectsCorruptedBytes(t *testing.T) {
	client, bucket := newTestBucket(t)
	store, err := New(client, Config{Bucket: bucket, Prefix: "integrity"})
	if err != nil {
		t.Fatal(err)
	}
	key, err := blobstore.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := store.Put(context.Background(), key, bytes.NewReader([]byte("trusted")), blobstore.PutOptions{
		MaxBytes: 64,
		Timeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	dataKey := "integrity/objects/" + key.String() + "/" + metadata.Version + "/data"
	if _, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(dataKey),
		Body:   bytes.NewReader([]byte("corrupt")),
	}); err != nil {
		t.Fatal(err)
	}
	reader, _, err := store.Open(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(reader)
	_ = reader.Close()
	if !errors.Is(err, blobstore.ErrIntegrity) {
		t.Fatalf("ReadAll() error = %v, want ErrIntegrity", err)
	}
}

func newTestBucket(t *testing.T) (*s3.Client, string) {
	t.Helper()
	endpoint := os.Getenv("OPEN_ASPM_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("OPEN_ASPM_TEST_S3_ENDPOINT is not set")
	}
	accessKey := os.Getenv("OPEN_ASPM_TEST_S3_ACCESS_KEY")
	secretKey := os.Getenv("OPEN_ASPM_TEST_S3_SECRET_KEY")
	if accessKey == "" || secretKey == "" {
		t.Fatal("S3 integration credentials are not set")
	}
	config := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	}
	client := s3.NewFromConfig(config, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
	bucket := "open-aspm-test-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create S3 test bucket: %v", err)
	}
	t.Cleanup(func() {
		cleanupBucket(client, bucket)
	})
	return client, bucket
}

func cleanupBucket(client *s3.Client, bucket string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var continuation *string
	for {
		output, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: continuation,
		})
		if err != nil {
			return
		}
		for _, object := range output.Contents {
			_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    object.Key,
			})
		}
		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuation = output.NextContinuationToken
	}
	_, _ = client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
}
