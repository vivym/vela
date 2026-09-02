package stageworkeragent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/stageassignment"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

type HTTPSRootInputResolverConfig struct {
	InputRoot string
	Client    *http.Client
}

type HTTPSRootInputResolver struct {
	inputRoot string
	client    *http.Client
}

func NewHTTPSRootInputResolver(
	config HTTPSRootInputResolverConfig,
) (*HTTPSRootInputResolver, error) {
	cleanedRoot := filepath.Clean(config.InputRoot)
	if !filepath.IsAbs(cleanedRoot) || cleanedRoot != config.InputRoot {
		return nil, errors.New("H3 root input resolver path is invalid")
	}
	if err := securefile.ValidateDirectory(cleanedRoot); err != nil {
		return nil, fmt.Errorf("validate H3 root input directory: %w", err)
	}
	client := config.Client
	if client == nil {
		client = &http.Client{}
	}
	cloned := *client
	originalRedirect := cloned.CheckRedirect
	cloned.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) >= 10 || !validHTTPSRootInputURL(request.URL) {
			return errors.New("H3 root input redirect is invalid")
		}
		if originalRedirect != nil {
			return originalRedirect(request, via)
		}
		return nil
	}
	return &HTTPSRootInputResolver{inputRoot: cleanedRoot, client: &cloned}, nil
}

func (resolver *HTTPSRootInputResolver) Resolve(
	ctx context.Context,
	assignment *velav1.StageAssignment,
) error {
	if resolver == nil || resolver.client == nil || ctx == nil {
		return errors.New("H3 root input resolver is not configured")
	}
	if _, err := stageassignment.Validate(assignment); err != nil {
		return fmt.Errorf("validate StageAssignment before root input resolution: %w", err)
	}
	inputs := assignment.GetExecutionSpec().GetRootInputs()
	if len(inputs) == 0 {
		return nil
	}
	stageRunID, err := uuid.Parse(assignment.GetAuthority().GetStageRunId())
	if err != nil || stageRunID == uuid.Nil {
		return errors.New("H3 root input resolution lacks StageRun identity")
	}
	root, err := securefile.OpenTrustedRoot(resolver.inputRoot)
	if err != nil {
		return fmt.Errorf("open H3 root input directory: %w", err)
	}
	defer func() { _ = root.Close() }()
	for index, input := range inputs {
		fetch := assignment.GetRootInputFetches()[index]
		var digest [sha256.Size]byte
		copy(digest[:], input.GetSha256())
		relativePath, pathErr := RootInputRelativePath(stageRunID, input.GetConditionIndex(), digest)
		if pathErr != nil {
			return pathErr
		}
		verified, verifyErr := verifyRootInputFile(root, relativePath, digest, input.GetSizeBytes())
		if verifyErr != nil {
			return fmt.Errorf("verify H3 root input %d: %w", index, verifyErr)
		}
		if verified {
			continue
		}
		if err := resolver.download(
			ctx, root, relativePath, fetch.GetDownloadUrl(), digest, input.GetSizeBytes(),
		); err != nil {
			return fmt.Errorf("materialize H3 root input %d: %w", index, err)
		}
	}
	return nil
}

func (resolver *HTTPSRootInputResolver) download(
	ctx context.Context,
	root *os.Root,
	finalPath string,
	downloadURL string,
	expectedDigest [sha256.Size]byte,
	expectedSize int64,
) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil || !validHTTPSRootInputURL(request.URL) {
		return errors.New("H3 root input download URL is invalid")
	}
	request.Header.Set("Accept-Encoding", "identity")
	response, err := resolver.client.Do(request)
	if err != nil {
		return fmt.Errorf("download H3 root input: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK || !validHTTPSRootInputURL(response.Request.URL) {
		return fmt.Errorf("H3 root input download returned status %d", response.StatusCode)
	}
	if response.ContentLength >= 0 && response.ContentLength != expectedSize {
		return errors.New("H3 root input Content-Length does not match execution spec")
	}
	directory := path.Dir(finalPath)
	if err := root.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create H3 root input directory: %w", err)
	}
	pendingPath := finalPath + ".partial." + uuid.NewString()
	file, err := root.OpenFile(pendingPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create H3 root input temporary file: %w", err)
	}
	committed := false
	defer func() {
		_ = file.Close()
		if !committed {
			_ = root.Remove(pendingPath)
		}
	}()
	digest := sha256.New()
	written, copyErr := io.Copy(
		io.MultiWriter(file, digest),
		io.LimitReader(response.Body, expectedSize+1),
	)
	syncErr := file.Sync()
	closeErr := file.Close()
	bodyCloseErr := response.Body.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil || bodyCloseErr != nil {
		return fmt.Errorf(
			"write H3 root input: %w",
			errors.Join(copyErr, syncErr, closeErr, bodyCloseErr),
		)
	}
	if written != expectedSize || !equalDigest(digest.Sum(nil), expectedDigest) {
		return errors.New("H3 root input failed exact integrity verification")
	}
	if err := root.Link(pendingPath, finalPath); err != nil {
		verified, verifyErr := verifyRootInputFile(root, finalPath, expectedDigest, expectedSize)
		if verifyErr != nil || !verified {
			return fmt.Errorf("publish H3 root input without replacement: %w", errors.Join(err, verifyErr))
		}
	}
	if err := root.Remove(pendingPath); err != nil {
		return fmt.Errorf("remove H3 root input temporary file: %w", err)
	}
	committed = true
	directoryHandle, err := root.Open(directory)
	if err != nil {
		return fmt.Errorf("open H3 root input directory for sync: %w", err)
	}
	syncErr = directoryHandle.Sync()
	closeErr = directoryHandle.Close()
	if syncErr != nil || closeErr != nil {
		return fmt.Errorf("sync H3 root input directory: %w", errors.Join(syncErr, closeErr))
	}
	return nil
}

func RootInputRelativePath(
	stageRunID uuid.UUID,
	conditionIndex int32,
	digest [sha256.Size]byte,
) (string, error) {
	if stageRunID == uuid.Nil || conditionIndex < 0 || digest == ([sha256.Size]byte{}) {
		return "", errors.New("H3 root input local path identity is invalid")
	}
	return path.Join(
		"stage-runs", stageRunID.String(), "root-inputs",
		fmt.Sprintf("%d", conditionIndex), hex.EncodeToString(digest[:])+".bin",
	), nil
}

func verifyRootInputFile(
	root *os.Root,
	relativePath string,
	expectedDigest [sha256.Size]byte,
	expectedSize int64,
) (bool, error) {
	pathInfo, err := root.Lstat(relativePath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !pathInfo.Mode().IsRegular() || pathInfo.Mode()&os.ModeSymlink != 0 ||
		pathInfo.Mode().Perm()&0o077 != 0 || pathInfo.Size() != expectedSize {
		return false, errors.New("H3 root input file metadata is invalid")
	}
	file, err := root.Open(relativePath)
	if err != nil {
		return false, err
	}
	openedInfo, statErr := file.Stat()
	digest := sha256.New()
	written, readErr := io.Copy(digest, io.LimitReader(file, expectedSize+1))
	closeErr := file.Close()
	if statErr != nil || !os.SameFile(pathInfo, openedInfo) || readErr != nil || closeErr != nil {
		return false, errors.Join(statErr, readErr, closeErr)
	}
	if written != expectedSize || !equalDigest(digest.Sum(nil), expectedDigest) {
		return false, errors.New("H3 root input file digest or size is invalid")
	}
	return true, nil
}

func validHTTPSRootInputURL(value *url.URL) bool {
	return value != nil && value.Scheme == "https" && value.Host != "" &&
		value.User == nil && value.Fragment == ""
}

type compositeInputResolver struct {
	resolvers []InputResolver
}

func NewCompositeInputResolver(resolvers ...InputResolver) (InputResolver, error) {
	if len(resolvers) == 0 {
		return nil, errors.New("composite input resolver is empty")
	}
	cloned := make([]InputResolver, len(resolvers))
	for index, resolver := range resolvers {
		if resolver == nil {
			return nil, errors.New("composite input resolver contains nil")
		}
		cloned[index] = resolver
	}
	return &compositeInputResolver{resolvers: cloned}, nil
}

func (resolver *compositeInputResolver) Resolve(
	ctx context.Context,
	assignment *velav1.StageAssignment,
) error {
	for _, child := range resolver.resolvers {
		if err := child.Resolve(ctx, assignment); err != nil {
			return err
		}
	}
	return nil
}

var _ InputResolver = (*HTTPSRootInputResolver)(nil)
var _ InputResolver = (*compositeInputResolver)(nil)
