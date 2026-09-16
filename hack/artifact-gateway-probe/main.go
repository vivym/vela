// Verify public, exact-version downloads with a caller-supplied test artifact.
// This is a transport probe, not a Vela Job or billing acceptance receipt.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/securefile"
)

func run() error {
	configPath := flag.String("configuration", "", "private S3Config JSON, including DownloadEndpoint")
	payloadPath := flag.String("payload", "", "complete test video file")
	caPath := flag.String("ca", "", "trusted gateway CA")
	output := flag.String("output", "", "new receipt directory")
	flag.Parse()
	if flag.NArg() != 0 || *configPath == "" || *payloadPath == "" || *caPath == "" || *output == "" {
		return errors.New("configuration, payload, ca and output are required")
	}
	wire, err := securefile.Read(*configPath, 64<<10, false)
	if err != nil {
		return err
	}
	var config artifactstore.S3Config
	if err = json.Unmarshal(wire, &config); err != nil {
		return err
	}
	clear(wire)
	if config.DownloadEndpoint == "" {
		return errors.New("public download endpoint is required")
	}
	payload, err := os.ReadFile(*payloadPath)
	if err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > 64<<20 {
		return errors.New("transport probe requires a complete 1..64MiB fixture")
	}
	digest := sha256.Sum256(payload)
	key := "artifacts/transport-acceptance/" + hex.EncodeToString(digest[:]) + "/video.mp4"
	if err = os.Mkdir(*output, 0700); err != nil {
		return err
	}
	store, err := artifactstore.NewS3(config)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err = store.ValidateBucket(ctx); err != nil {
		return err
	}
	version, err := store.PutIfAbsent(ctx, key, "video/mp4", bytes.NewReader(payload), int64(len(payload)), digest)
	if errors.Is(err, artifactstore.ErrObjectAlreadyExists) {
		var exists bool
		version, exists, err = store.ResolveCurrentVersion(ctx, key)
		if err == nil && !exists {
			return errors.New("existing probe object lost its version")
		}
	}
	if err != nil {
		return err
	}
	signed, err := store.PresignExactVersion(ctx, key, version.VersionID)
	if err != nil {
		return err
	}
	ca, err := os.ReadFile(*caPath)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("gateway CA is invalid")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Get(signed.URL)
	if err != nil {
		return errors.New("gateway download failed")
	}
	downloaded, readErr := io.ReadAll(io.LimitReader(response.Body, int64(len(payload))+1))
	_ = response.Body.Close()
	if readErr != nil || response.StatusCode != http.StatusOK || !bytes.Equal(downloaded, payload) {
		return fmt.Errorf("gateway download mismatch, HTTP %d", response.StatusCode)
	}
	if err = os.WriteFile(filepath.Join(*output, "complete-video.mp4"), downloaded, 0600); err != nil {
		return err
	}
	negatives := map[string]int{}
	for _, kind := range []string{"unsigned", "signature", "version"} {
		u, err := url.Parse(signed.URL)
		if err != nil {
			return err
		}
		q := u.Query()
		switch kind {
		case "unsigned":
			q = url.Values{"versionId": {version.VersionID}}
		case "signature":
			q.Set("X-Amz-Signature", strings.Repeat("0", 64))
		case "version":
			q.Set("versionId", "00000000-0000-0000-0000-000000000000")
		}
		u.RawQuery = q.Encode()
		r, err := client.Get(u.String())
		if err != nil {
			return errors.New("negative download probe failed")
		}
		_ = r.Body.Close()
		negatives[kind] = r.StatusCode
		if r.StatusCode != http.StatusForbidden {
			return fmt.Errorf("%s negative probe HTTP %d", kind, r.StatusCode)
		}
	}
	receipt := map[string]any{"schema_version": 1, "scope": "artifact-transport-only", "job_acceptance": false, "billing_acceptance": false, "observed_at": time.Now().UTC(), "download_origin": config.DownloadEndpoint, "object_key": key, "object_version": version.VersionID, "sha256": hex.EncodeToString(digest[:]), "size_bytes": len(payload), "tls_verified": true, "download_http": response.StatusCode, "negative_http": negatives}
	receiptBytes, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(*output, "receipt.json"), append(receiptBytes, '\n'), 0600); err != nil {
		return err
	}
	_, err = fmt.Println(string(receiptBytes))
	return err
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
