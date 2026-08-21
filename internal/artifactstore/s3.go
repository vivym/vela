package artifactstore

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const (
	MaxSignedGETTTL    = 15 * time.Minute
	MaxSignedUploadTTL = 15 * time.Minute
)

const maxIncompleteMultipartUploads = 10_000

var (
	ErrObjectAlreadyExists      = errors.New("artifact object already exists")
	ErrBucketVersioningRequired = errors.New("artifact bucket versioning is required")
	ErrBucketNotPrivate         = errors.New("artifact bucket must be private")
)

type S3Config struct {
	Endpoint        string
	Region          string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	UsePathStyle    bool
	SignedGETTTL    time.Duration
}

type S3 struct {
	client       *s3.Client
	presign      *s3.PresignClient
	bucket       string
	signedGETTTL time.Duration
	now          func() time.Time
}

type MultipartUpload struct {
	ObjectKey   string
	UploadID    string
	ContentType string
}

type CompletedPart struct {
	Number         int32
	ETag           string
	SizeBytes      int64
	ChecksumSHA256 string
}

func EqualCompletedParts(left []CompletedPart, right []CompletedPart) bool {
	if len(left) == 0 || len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func MultipartCompositeChecksum(parts []CompletedPart) (string, error) {
	if len(parts) == 0 || len(parts) > 10_000 {
		return "", errors.New("invalid multipart checksum input")
	}
	digest := sha256.New()
	for _, part := range parts {
		partDigest, err := base64.StdEncoding.DecodeString(part.ChecksumSHA256)
		if err != nil || len(partDigest) != sha256.Size {
			return "", errors.New("invalid multipart part checksum")
		}
		_, _ = digest.Write(partDigest)
	}
	return base64.StdEncoding.EncodeToString(digest.Sum(nil)) + "-" + fmt.Sprint(len(parts)), nil
}

type IncompleteMultipartUpload struct {
	ObjectKey   string
	UploadID    string
	InitiatedAt time.Time
}

type ObjectVersion struct {
	ObjectKey      string
	VersionID      string
	ETag           string
	SizeBytes      int64
	ContentType    string
	ChecksumSHA256 string
}

type ExactVersionReader struct {
	io.ReadCloser
	ObjectVersion
}

type SignedRead struct {
	URL       string
	IssuedAt  time.Time
	ExpiresAt time.Time
}

type SignedUploadPart struct {
	URL       string
	Method    string
	Headers   http.Header
	IssuedAt  time.Time
	ExpiresAt time.Time
}

func NewS3(config S3Config) (*S3, error) {
	endpoint := strings.TrimSpace(config.Endpoint)
	region := strings.TrimSpace(config.Region)
	bucket := strings.TrimSpace(config.Bucket)
	accessKeyID := strings.TrimSpace(config.AccessKeyID)
	if endpoint == "" || region == "" || bucket == "" || accessKeyID == "" ||
		config.SecretAccessKey == "" || config.SignedGETTTL <= 0 ||
		config.SignedGETTTL > MaxSignedGETTTL {
		return nil, errors.New("invalid S3 Artifact Store configuration")
	}
	parsedEndpoint, err := url.Parse(endpoint)
	if err != nil || (parsedEndpoint.Scheme != "http" && parsedEndpoint.Scheme != "https") ||
		parsedEndpoint.Host == "" || parsedEndpoint.User != nil || parsedEndpoint.RawQuery != "" ||
		parsedEndpoint.Fragment != "" {
		return nil, errors.New("invalid S3 Artifact Store endpoint")
	}

	awsConfig := aws.Config{
		Region: region,
		Credentials: credentials.NewStaticCredentialsProvider(
			accessKeyID,
			config.SecretAccessKey,
			"",
		),
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(strings.TrimRight(endpoint, "/"))
		options.UsePathStyle = config.UsePathStyle
	})
	return &S3{
		client:       client,
		presign:      s3.NewPresignClient(client),
		bucket:       bucket,
		signedGETTTL: config.SignedGETTTL,
		now:          time.Now,
	}, nil
}

func (store *S3) ValidateBucket(ctx context.Context) error {
	if store == nil || store.client == nil || store.bucket == "" {
		return errors.New("S3 Artifact Store is not configured")
	}
	versioning, err := store.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: aws.String(store.bucket),
	})
	if err != nil {
		return fmt.Errorf("read Artifact bucket versioning: %w", err)
	}
	if versioning.Status != types.BucketVersioningStatusEnabled {
		return ErrBucketVersioningRequired
	}
	acl, err := store.client.GetBucketAcl(ctx, &s3.GetBucketAclInput{
		Bucket: aws.String(store.bucket),
	})
	if err != nil {
		return fmt.Errorf("read Artifact bucket ACL: %w", err)
	}
	for _, grant := range acl.Grants {
		if grant.Grantee == nil || grant.Grantee.URI == nil {
			continue
		}
		uri := *grant.Grantee.URI
		if strings.HasSuffix(uri, "/AllUsers") || strings.HasSuffix(uri, "/AuthenticatedUsers") {
			return ErrBucketNotPrivate
		}
	}
	policy, err := store.client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{
		Bucket: aws.String(store.bucket),
	})
	if err != nil {
		if isAPIErrorCode(err, "NoSuchBucketPolicy", "NoSuchPolicy") {
			return nil
		}
		return fmt.Errorf("read Artifact bucket policy: %w", err)
	}
	if policy.Policy == nil || *policy.Policy == "" {
		return nil
	}
	public, err := policyAllowsPublicPrincipal([]byte(*policy.Policy))
	if err != nil {
		return fmt.Errorf("validate Artifact bucket policy: %w", err)
	}
	if public {
		return ErrBucketNotPrivate
	}
	return nil
}

func (store *S3) CreateMultipartUpload(
	ctx context.Context,
	objectKey string,
	contentType string,
) (MultipartUpload, error) {
	if err := validateObjectKey(objectKey); err != nil {
		return MultipartUpload{}, err
	}
	contentType = strings.TrimSpace(contentType)
	if contentType == "" || len(contentType) > 200 || strings.ContainsRune(contentType, '\x00') {
		return MultipartUpload{}, errors.New("invalid Artifact content type")
	}
	output, err := store.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:            aws.String(store.bucket),
		Key:               aws.String(objectKey),
		ContentType:       aws.String(contentType),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		return MultipartUpload{}, fmt.Errorf("create S3 multipart upload: %w", err)
	}
	if output.UploadId == nil || *output.UploadId == "" {
		return MultipartUpload{}, errors.New("S3 multipart upload returned no upload ID")
	}
	return MultipartUpload{
		ObjectKey:   objectKey,
		UploadID:    *output.UploadId,
		ContentType: contentType,
	}, nil
}

func (store *S3) UploadPart(
	ctx context.Context,
	upload MultipartUpload,
	partNumber int32,
	body io.Reader,
	sizeBytes int64,
	digest [sha256.Size]byte,
) (CompletedPart, error) {
	if err := validateMultipartUpload(upload); err != nil {
		return CompletedPart{}, err
	}
	if partNumber <= 0 || partNumber > 10_000 || body == nil || sizeBytes <= 0 ||
		digest == [sha256.Size]byte{} {
		return CompletedPart{}, errors.New("invalid S3 multipart part")
	}
	checksum := base64.StdEncoding.EncodeToString(digest[:])
	output, err := store.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:         aws.String(store.bucket),
		Key:            aws.String(upload.ObjectKey),
		UploadId:       aws.String(upload.UploadID),
		PartNumber:     aws.Int32(partNumber),
		Body:           body,
		ContentLength:  aws.Int64(sizeBytes),
		ChecksumSHA256: aws.String(checksum),
	})
	if err != nil {
		return CompletedPart{}, fmt.Errorf("upload S3 multipart part: %w", err)
	}
	if output.ETag == nil || *output.ETag == "" {
		return CompletedPart{}, errors.New("S3 multipart part returned no ETag")
	}
	return CompletedPart{
		Number:         partNumber,
		ETag:           *output.ETag,
		SizeBytes:      sizeBytes,
		ChecksumSHA256: checksum,
	}, nil
}

func (store *S3) PresignUploadPart(
	ctx context.Context,
	upload MultipartUpload,
	partNumber int32,
	sizeBytes int64,
	digest [sha256.Size]byte,
	expiresAt time.Time,
) (SignedUploadPart, error) {
	if store == nil || store.presign == nil || store.now == nil {
		return SignedUploadPart{}, errors.New("S3 Artifact Store is not configured")
	}
	if err := validateMultipartUpload(upload); err != nil {
		return SignedUploadPart{}, err
	}
	if ctx == nil || partNumber <= 0 || partNumber > 10_000 || sizeBytes <= 0 ||
		digest == [sha256.Size]byte{} {
		return SignedUploadPart{}, errors.New("invalid signed S3 multipart part")
	}
	issuedAt := store.now().UTC()
	if !expiresAt.After(issuedAt) {
		return SignedUploadPart{}, errors.New("signed S3 multipart part expiry has elapsed")
	}
	maximumExpiry := issuedAt.Add(MaxSignedUploadTTL)
	if expiresAt.After(maximumExpiry) {
		expiresAt = maximumExpiry
	}
	checksum := base64.StdEncoding.EncodeToString(digest[:])
	output, err := store.presign.PresignUploadPart(
		ctx,
		&s3.UploadPartInput{
			Bucket:         aws.String(store.bucket),
			Key:            aws.String(upload.ObjectKey),
			UploadId:       aws.String(upload.UploadID),
			PartNumber:     aws.Int32(partNumber),
			ContentLength:  aws.Int64(sizeBytes),
			ChecksumSHA256: aws.String(checksum),
		},
		s3.WithPresignExpires(expiresAt.Sub(issuedAt)),
	)
	if err != nil {
		return SignedUploadPart{}, fmt.Errorf("sign S3 multipart part: %w", err)
	}
	if output == nil || output.URL == "" || output.Method != http.MethodPut ||
		output.SignedHeader.Get("Content-Length") == "" ||
		output.SignedHeader.Get("X-Amz-Checksum-Sha256") != checksum {
		return SignedUploadPart{}, errors.New("signed S3 multipart part is incomplete")
	}
	return SignedUploadPart{
		URL:       output.URL,
		Method:    output.Method,
		Headers:   output.SignedHeader.Clone(),
		IssuedAt:  issuedAt,
		ExpiresAt: expiresAt,
	}, nil
}

func (store *S3) ListParts(
	ctx context.Context,
	upload MultipartUpload,
) ([]CompletedPart, error) {
	if err := validateMultipartUpload(upload); err != nil {
		return nil, err
	}
	paginator := s3.NewListPartsPaginator(store.client, &s3.ListPartsInput{
		Bucket:   aws.String(store.bucket),
		Key:      aws.String(upload.ObjectKey),
		UploadId: aws.String(upload.UploadID),
	})
	parts := make([]CompletedPart, 0)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list S3 multipart parts: %w", err)
		}
		for _, part := range page.Parts {
			if part.PartNumber == nil || part.ETag == nil || part.Size == nil ||
				*part.PartNumber <= 0 || *part.ETag == "" || *part.Size <= 0 {
				return nil, errors.New("S3 multipart part identity is incomplete")
			}
			parts = append(parts, CompletedPart{
				Number:         *part.PartNumber,
				ETag:           *part.ETag,
				SizeBytes:      *part.Size,
				ChecksumSHA256: aws.ToString(part.ChecksumSHA256),
			})
		}
	}
	sort.Slice(parts, func(left, right int) bool {
		return parts[left].Number < parts[right].Number
	})
	return parts, nil
}

func (store *S3) ListIncompleteMultipartUploads(
	ctx context.Context,
	objectPrefix string,
) ([]IncompleteMultipartUpload, error) {
	if err := validateObjectPrefix(objectPrefix); err != nil {
		return nil, err
	}
	paginator := s3.NewListMultipartUploadsPaginator(
		store.client,
		&s3.ListMultipartUploadsInput{
			Bucket: aws.String(store.bucket),
		},
	)
	uploads := make([]IncompleteMultipartUpload, 0)
	listed := 0
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list incomplete S3 multipart uploads: %w", err)
		}
		for _, upload := range page.Uploads {
			listed++
			if listed > maxIncompleteMultipartUploads {
				return nil, errors.New("too many incomplete S3 multipart uploads")
			}
			if upload.Key == nil || upload.UploadId == nil || upload.Initiated == nil ||
				*upload.Key == "" || *upload.UploadId == "" {
				return nil, errors.New("incomplete S3 multipart upload identity is incomplete")
			}
			if err := validateObjectKey(*upload.Key); err != nil {
				return nil, fmt.Errorf("validate incomplete S3 multipart upload key: %w", err)
			}
			if !strings.HasPrefix(*upload.Key, objectPrefix) {
				continue
			}
			uploads = append(uploads, IncompleteMultipartUpload{
				ObjectKey:   *upload.Key,
				UploadID:    *upload.UploadId,
				InitiatedAt: *upload.Initiated,
			})
		}
	}
	sort.Slice(uploads, func(left, right int) bool {
		if uploads[left].InitiatedAt.Equal(uploads[right].InitiatedAt) {
			if uploads[left].ObjectKey == uploads[right].ObjectKey {
				return uploads[left].UploadID < uploads[right].UploadID
			}
			return uploads[left].ObjectKey < uploads[right].ObjectKey
		}
		return uploads[left].InitiatedAt.Before(uploads[right].InitiatedAt)
	})
	return uploads, nil
}

func (store *S3) AbortMultipartUpload(ctx context.Context, upload MultipartUpload) error {
	if err := validateMultipartUpload(upload); err != nil {
		return err
	}
	_, err := store.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(store.bucket),
		Key:      aws.String(upload.ObjectKey),
		UploadId: aws.String(upload.UploadID),
	})
	if err != nil && !isAPIErrorCode(err, "NoSuchUpload", "404") {
		return fmt.Errorf("abort S3 multipart upload: %w", err)
	}
	return nil
}

func (store *S3) CompleteMultipartUpload(
	ctx context.Context,
	upload MultipartUpload,
	parts []CompletedPart,
) (ObjectVersion, error) {
	if err := validateMultipartUpload(upload); err != nil {
		return ObjectVersion{}, err
	}
	completed, err := completedS3Parts(parts)
	if err != nil {
		return ObjectVersion{}, err
	}
	output, err := store.client.CompleteMultipartUpload(
		ctx,
		&s3.CompleteMultipartUploadInput{
			Bucket:      aws.String(store.bucket),
			Key:         aws.String(upload.ObjectKey),
			UploadId:    aws.String(upload.UploadID),
			IfNoneMatch: aws.String("*"),
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: completed,
			},
		},
	)
	if err != nil {
		if isPreconditionFailed(err) {
			return ObjectVersion{}, ErrObjectAlreadyExists
		}
		return ObjectVersion{}, fmt.Errorf("complete S3 multipart upload: %w", err)
	}
	if output.VersionId == nil || *output.VersionId == "" {
		return ObjectVersion{}, errors.New("completed S3 object has no version ID")
	}
	return store.headExactVersion(ctx, upload.ObjectKey, *output.VersionId)
}

func (store *S3) PutIfAbsent(
	ctx context.Context,
	objectKey string,
	contentType string,
	body io.Reader,
	sizeBytes int64,
	digest [sha256.Size]byte,
) (ObjectVersion, error) {
	if err := validateObjectKey(objectKey); err != nil {
		return ObjectVersion{}, err
	}
	contentType = strings.TrimSpace(contentType)
	if contentType == "" || len(contentType) > 200 || strings.ContainsRune(contentType, '\x00') ||
		body == nil || sizeBytes <= 0 || digest == [sha256.Size]byte{} {
		return ObjectVersion{}, errors.New("invalid conditional Artifact object")
	}
	checksum := base64.StdEncoding.EncodeToString(digest[:])
	output, err := store.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:         aws.String(store.bucket),
		Key:            aws.String(objectKey),
		Body:           body,
		ContentLength:  aws.Int64(sizeBytes),
		ContentType:    aws.String(contentType),
		ChecksumSHA256: aws.String(checksum),
		IfNoneMatch:    aws.String("*"),
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return ObjectVersion{}, ErrObjectAlreadyExists
		}
		return ObjectVersion{}, fmt.Errorf("conditionally create S3 Artifact object: %w", err)
	}
	if output.VersionId == nil || *output.VersionId == "" {
		return ObjectVersion{}, errors.New("conditionally created S3 object has no version ID")
	}
	return store.headExactVersion(ctx, objectKey, *output.VersionId)
}

func (store *S3) ReadExactVersion(
	ctx context.Context,
	objectKey string,
	versionID string,
) (*ExactVersionReader, error) {
	if err := validateExactVersion(objectKey, versionID); err != nil {
		return nil, err
	}
	output, err := store.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       aws.String(store.bucket),
		Key:          aws.String(objectKey),
		VersionId:    aws.String(versionID),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return nil, fmt.Errorf("read exact S3 Artifact version: %w", err)
	}
	if output.Body == nil || output.VersionId == nil || *output.VersionId != versionID ||
		output.ContentLength == nil || *output.ContentLength <= 0 || output.ContentType == nil {
		if output.Body != nil {
			_ = output.Body.Close()
		}
		return nil, errors.New("exact S3 Artifact version metadata is incomplete")
	}
	return &ExactVersionReader{
		ReadCloser: output.Body,
		ObjectVersion: ObjectVersion{
			ObjectKey:      objectKey,
			VersionID:      versionID,
			ETag:           aws.ToString(output.ETag),
			SizeBytes:      *output.ContentLength,
			ContentType:    *output.ContentType,
			ChecksumSHA256: aws.ToString(output.ChecksumSHA256),
		},
	}, nil
}

func (store *S3) HeadCurrentVersion(
	ctx context.Context,
	objectKey string,
) (ObjectVersion, error) {
	if err := validateObjectKey(objectKey); err != nil {
		return ObjectVersion{}, err
	}
	output, err := store.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(store.bucket),
		Key:          aws.String(objectKey),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return ObjectVersion{}, fmt.Errorf("resolve current S3 Artifact version: %w", err)
	}
	if output.VersionId == nil || *output.VersionId == "" || output.ContentLength == nil ||
		*output.ContentLength <= 0 || output.ContentType == nil || *output.ContentType == "" {
		return ObjectVersion{}, errors.New("current S3 Artifact version metadata is incomplete")
	}
	return ObjectVersion{
		ObjectKey:      objectKey,
		VersionID:      *output.VersionId,
		ETag:           aws.ToString(output.ETag),
		SizeBytes:      *output.ContentLength,
		ContentType:    *output.ContentType,
		ChecksumSHA256: aws.ToString(output.ChecksumSHA256),
	}, nil
}

func (store *S3) PresignExactVersion(
	ctx context.Context,
	objectKey string,
	versionID string,
) (SignedRead, error) {
	if err := validateExactVersion(objectKey, versionID); err != nil {
		return SignedRead{}, err
	}
	issuedAt := store.now().UTC()
	output, err := store.presign.PresignGetObject(
		ctx,
		&s3.GetObjectInput{
			Bucket:    aws.String(store.bucket),
			Key:       aws.String(objectKey),
			VersionId: aws.String(versionID),
		},
		s3.WithPresignExpires(store.signedGETTTL),
	)
	if err != nil {
		return SignedRead{}, fmt.Errorf("presign exact S3 Artifact version: %w", err)
	}
	if output.URL == "" {
		return SignedRead{}, errors.New("presigned S3 Artifact URL is empty")
	}
	return SignedRead{
		URL:       output.URL,
		IssuedAt:  issuedAt,
		ExpiresAt: issuedAt.Add(store.signedGETTTL),
	}, nil
}

func (store *S3) headExactVersion(
	ctx context.Context,
	objectKey string,
	versionID string,
) (ObjectVersion, error) {
	output, err := store.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(store.bucket),
		Key:          aws.String(objectKey),
		VersionId:    aws.String(versionID),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return ObjectVersion{}, fmt.Errorf("resolve exact S3 Artifact version: %w", err)
	}
	if output.VersionId == nil || *output.VersionId != versionID || output.ContentLength == nil ||
		*output.ContentLength <= 0 || output.ContentType == nil || *output.ContentType == "" {
		return ObjectVersion{}, errors.New("exact S3 Artifact version metadata is incomplete")
	}
	return ObjectVersion{
		ObjectKey:      objectKey,
		VersionID:      versionID,
		ETag:           aws.ToString(output.ETag),
		SizeBytes:      *output.ContentLength,
		ContentType:    *output.ContentType,
		ChecksumSHA256: aws.ToString(output.ChecksumSHA256),
	}, nil
}

func completedS3Parts(parts []CompletedPart) ([]types.CompletedPart, error) {
	if len(parts) == 0 || len(parts) > 10_000 {
		return nil, errors.New("invalid completed S3 multipart parts")
	}
	completed := make([]types.CompletedPart, len(parts))
	for index, part := range parts {
		if part.Number != int32(index+1) || part.ETag == "" || part.SizeBytes <= 0 ||
			part.ChecksumSHA256 == "" {
			return nil, errors.New("invalid completed S3 multipart part")
		}
		completed[index] = types.CompletedPart{
			PartNumber:     aws.Int32(part.Number),
			ETag:           aws.String(part.ETag),
			ChecksumSHA256: aws.String(part.ChecksumSHA256),
		}
	}
	return completed, nil
}

func validateMultipartUpload(upload MultipartUpload) error {
	if err := validateObjectKey(upload.ObjectKey); err != nil {
		return err
	}
	if upload.UploadID == "" || len(upload.UploadID) > 2000 ||
		strings.ContainsRune(upload.UploadID, '\x00') {
		return errors.New("invalid S3 multipart upload ID")
	}
	return nil
}

func validateExactVersion(objectKey string, versionID string) error {
	if err := validateObjectKey(objectKey); err != nil {
		return err
	}
	if versionID == "" || len(versionID) > 1000 || strings.ContainsRune(versionID, '\x00') {
		return errors.New("invalid S3 Artifact version ID")
	}
	return nil
}

func validateObjectKey(objectKey string) error {
	if !strings.HasPrefix(objectKey, "artifacts/") || len(objectKey) > 1024 ||
		strings.HasSuffix(objectKey, "/") || strings.ContainsRune(objectKey, '\x00') ||
		strings.Contains(objectKey, "//") {
		return errors.New("invalid Artifact object key")
	}
	return nil
}

func validateObjectPrefix(objectPrefix string) error {
	if !strings.HasPrefix(objectPrefix, "artifacts/") || len(objectPrefix) > 1024 ||
		strings.ContainsRune(objectPrefix, '\x00') || strings.Contains(objectPrefix, "//") {
		return errors.New("invalid Artifact object prefix")
	}
	return nil
}

func isPreconditionFailed(err error) bool {
	return isAPIErrorCode(err, "PreconditionFailed", "412")
}

func isAPIErrorCode(err error, codes ...string) bool {
	var apiError smithy.APIError
	if !errors.As(err, &apiError) {
		return false
	}
	for _, code := range codes {
		if apiError.ErrorCode() == code {
			return true
		}
	}
	return false
}

func policyAllowsPublicPrincipal(document []byte) (bool, error) {
	var policy struct {
		Statement json.RawMessage `json:"Statement"`
	}
	if err := json.Unmarshal(document, &policy); err != nil || len(policy.Statement) == 0 {
		return false, errors.New("invalid S3 bucket policy document")
	}
	type statement struct {
		Effect       string          `json:"Effect"`
		Principal    any             `json:"Principal"`
		NotPrincipal json.RawMessage `json:"NotPrincipal"`
	}
	var statements []statement
	switch policy.Statement[0] {
	case '[':
		if err := json.Unmarshal(policy.Statement, &statements); err != nil {
			return false, errors.New("invalid S3 bucket policy statements")
		}
	case '{':
		var single statement
		if err := json.Unmarshal(policy.Statement, &single); err != nil {
			return false, errors.New("invalid S3 bucket policy statement")
		}
		statements = []statement{single}
	default:
		return false, errors.New("invalid S3 bucket policy statement shape")
	}
	for _, candidate := range statements {
		if candidate.Effect == "Allow" &&
			(len(candidate.NotPrincipal) != 0 || containsPublicPrincipal(candidate.Principal)) {
			return true, nil
		}
	}
	return false, nil
}

func containsPublicPrincipal(value any) bool {
	switch typed := value.(type) {
	case string:
		return typed == "*"
	case []any:
		for _, item := range typed {
			if containsPublicPrincipal(item) {
				return true
			}
		}
	case map[string]any:
		for _, item := range typed {
			if containsPublicPrincipal(item) {
				return true
			}
		}
	}
	return false
}
