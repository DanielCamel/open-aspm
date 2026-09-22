package s3store

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/spectremi/open-aspm/internal/blobstore"
)

func TestSafeS3ErrorRedactsBackendDetails(t *testing.T) {
	errorWithSecret := errors.New("request to secret-bucket failed with credential SECRET")
	err := safeS3Error("upload object", errorWithSecret)
	if !errors.Is(err, blobstore.ErrBackend) {
		t.Fatalf("safeS3Error() = %v, want ErrBackend", err)
	}
	if strings.Contains(err.Error(), "secret-bucket") || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("safeS3Error() leaked backend details: %v", err)
	}
}

func TestNewRejectsUnsafePrefix(t *testing.T) {
	for _, prefix := range []string{"../escape", "safe/../escape", `safe\\escape`} {
		if _, err := New(&s3.Client{}, Config{Bucket: "bucket", Prefix: prefix}); !errors.Is(err, blobstore.ErrInvalidOptions) {
			t.Errorf("New(prefix %q) error = %v, want ErrInvalidOptions", prefix, err)
		}
	}
}
