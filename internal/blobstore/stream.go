package blobstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
)

// BoundedReader computes SHA-256 while enforcing context and byte limits.
type BoundedReader struct {
	ctx      context.Context
	source   io.Reader
	hash     hash.Hash
	maxBytes int64
	size     int64
	finished bool
	err      error
}

// NewBoundedReader wraps an untrusted stream.
func NewBoundedReader(ctx context.Context, source io.Reader, maxBytes int64) *BoundedReader {
	return &BoundedReader{
		ctx:      ctx,
		source:   source,
		hash:     sha256.New(),
		maxBytes: maxBytes,
	}
}

// Read implements io.Reader. It never returns a byte beyond maxBytes.
func (reader *BoundedReader) Read(buffer []byte) (int, error) {
	if reader.err != nil {
		return 0, reader.err
	}
	if reader.finished {
		return 0, io.EOF
	}
	if err := ContextError(reader.ctx); err != nil {
		reader.err = err
		return 0, err
	}

	remaining := reader.maxBytes - reader.size
	readBuffer := buffer
	if int64(len(readBuffer)) > remaining+1 {
		readBuffer = readBuffer[:remaining+1]
	}
	count, err := reader.source.Read(readBuffer)
	if contextErr := ContextError(reader.ctx); contextErr != nil {
		reader.err = contextErr
		return 0, contextErr
	}
	if int64(count) > remaining {
		reader.err = ErrTooLarge
		return 0, reader.err
	}
	if count > 0 {
		_, _ = reader.hash.Write(readBuffer[:count])
		reader.size += int64(count)
	}
	if errors.Is(err, io.EOF) {
		reader.finished = true
	}
	if err != nil && !errors.Is(err, io.EOF) {
		reader.err = err
	}
	return count, err
}

// Result returns the size and digest after EOF was observed.
func (reader *BoundedReader) Result() (int64, string, error) {
	if reader.err != nil {
		return 0, "", reader.err
	}
	if !reader.finished {
		return 0, "", errors.New("blob stream was not consumed")
	}
	return reader.size, hex.EncodeToString(reader.hash.Sum(nil)), nil
}

// Err reports a terminal limit, context, or source error observed while reading.
func (reader *BoundedReader) Err() error {
	return reader.err
}

// VerifyExpected compares a completed stream with client assertions.
func VerifyExpected(size int64, digest string, options PutOptions) error {
	if options.ExpectedSize != nil && size != *options.ExpectedSize {
		return ErrIntegrity
	}
	if options.ExpectedSHA256 != "" && digest != options.ExpectedSHA256 {
		return ErrIntegrity
	}
	return nil
}

// NewVerifiedReader returns a stream that validates committed metadata at EOF.
func NewVerifiedReader(
	ctx context.Context,
	source io.ReadCloser,
	expectedSize int64,
	expectedSHA256 string,
) io.ReadCloser {
	return &verifiedReader{
		ctx:            ctx,
		source:         source,
		hash:           sha256.New(),
		expectedSize:   expectedSize,
		expectedSHA256: expectedSHA256,
	}
}

type verifiedReader struct {
	ctx            context.Context
	source         io.ReadCloser
	hash           hash.Hash
	size           int64
	expectedSize   int64
	expectedSHA256 string
	finished       bool
}

func (reader *verifiedReader) Read(buffer []byte) (int, error) {
	if reader.finished {
		return 0, io.EOF
	}
	if err := ContextError(reader.ctx); err != nil {
		return 0, err
	}
	count, err := reader.source.Read(buffer)
	if count > 0 {
		_, _ = reader.hash.Write(buffer[:count])
		reader.size += int64(count)
		if reader.size > reader.expectedSize {
			reader.finished = true
			return count, ErrIntegrity
		}
	}
	if errors.Is(err, io.EOF) {
		reader.finished = true
		digest := hex.EncodeToString(reader.hash.Sum(nil))
		if reader.size != reader.expectedSize || digest != reader.expectedSHA256 {
			return count, ErrIntegrity
		}
	}
	return count, err
}

func (reader *verifiedReader) Close() error {
	return reader.source.Close()
}
