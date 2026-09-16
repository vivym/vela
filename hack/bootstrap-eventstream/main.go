// Bootstrap the release-owned event stream and consumer without mutating drifted resources.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vivym/vela/internal/eventstream"
)

func kube(kind, name string, dst any) error {
	args := []string{"--request-timeout=15s", "-n", "vela-system", "get", kind}
	if name != "" {
		args = append(args, name)
	}
	args = append(args, "-o", "json")
	b, e := exec.Command("kubectl", args...).Output()
	if e != nil {
		return fmt.Errorf("read %s: %w", kind, e)
	}
	return json.Unmarshal(b, dst)
}
func connect(role string) (*nats.Conn, func(), error) {
	var auth, tlsSecret struct{ Data map[string][]byte }
	if e := kube("secret", "vela-nats-auth", &auth); e != nil {
		return nil, nil, e
	}
	if e := kube("secret", "nats-client-tls", &tlsSecret); e != nil {
		return nil, nil, e
	}
	cert, e := tls.X509KeyPair(tlsSecret.Data["tls.crt"], tlsSecret.Data["tls.key"])
	if e != nil {
		return nil, nil, e
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(tlsSecret.Data["ca.crt"]) {
		return nil, nil, errors.New("no NATS CA")
	}
	dir, e := os.MkdirTemp("/run", "vela-nats-probe-")
	if e != nil {
		return nil, nil, e
	}
	cleanup := func() {
		if err := os.RemoveAll(dir); err != nil {
			fmt.Fprintf(os.Stderr, "remove NATS probe credential directory: %v\n", err)
		}
	}
	path := filepath.Join(dir, "client.creds")
	if e = os.WriteFile(path, auth.Data[role+".creds"], 0600); e != nil {
		cleanup()
		return nil, nil, e
	}
	var pods struct {
		Items []struct {
			Metadata struct{ Labels map[string]string }
			Status   struct {
				PodIP string
				Phase string
			}
		}
	}
	if e = kube("pods", "", &pods); e != nil {
		cleanup()
		return nil, nil, e
	}
	var servers []string
	for _, p := range pods.Items {
		if p.Metadata.Labels["app.kubernetes.io/name"] == "nats" && p.Status.PodIP != "" && p.Status.Phase == "Running" {
			servers = append(servers, "tls://"+p.Status.PodIP+":4222")
		}
	}
	if len(servers) == 0 {
		cleanup()
		return nil, nil, errors.New("no running NATS server")
	}
	nc, e := nats.Connect(strings.Join(servers, ","), nats.UserCredentials(path), nats.Secure(&tls.Config{MinVersion: tls.VersionTLS13, ServerName: "nats.vela-system.svc", RootCAs: roots, Certificates: []tls.Certificate{cert}}), nats.Timeout(4*time.Second), nats.MaxReconnects(3), nats.ReconnectWait(time.Second))
	if e != nil {
		cleanup()
		return nil, nil, e
	}
	return nc, func() { nc.Close(); cleanup() }, nil
}

func run() error {
	apply := flag.Bool("apply", false, "create missing release stream/consumer; never update drift")
	flag.Parse()
	if flag.NArg() != 0 {
		return errors.New("unexpected bootstrap arguments")
	}
	nc, closeConnection, err := connect("bootstrap")
	if err != nil {
		return err
	}
	defer closeConnection()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, eventstream.StreamName)
	createdStream := false
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		if !*apply {
			return errors.New("release stream is missing; rerun with --apply after verifying per-server quota")
		}
		if err := preflightCapacity(ctx); err != nil {
			return err
		}
		stream, err = js.CreateStream(ctx, eventstream.StreamConfig())
		createdStream = err == nil
	}
	if err != nil {
		return err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return err
	}
	if err = eventstream.ValidateStreamConfig(info.Config); err != nil {
		return err
	}
	consumer, err := stream.Consumer(ctx, eventstream.SchedulerConsumerName)
	createdConsumer := false
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		if !*apply {
			return errors.New("release consumer is missing; rerun with --apply")
		}
		consumer, err = stream.CreateConsumer(ctx, eventstream.SchedulerConsumerConfig())
		createdConsumer = err == nil
	}
	if err != nil {
		return err
	}
	ci, err := consumer.Info(ctx)
	if err != nil {
		return err
	}
	if err = eventstream.ValidateSchedulerConsumerConfig(ci.Config); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		CreatedStream   bool                    `json:"created_stream"`
		CreatedConsumer bool                    `json:"created_consumer"`
		Stream          *jetstream.StreamInfo   `json:"stream"`
		Consumer        *jetstream.ConsumerInfo `json:"consumer"`
	}{createdStream, createdConsumer, info, ci})
}

type memberCapacity struct {
	Name   string `json:"-"`
	Config struct {
		MaxStorage uint64 `json:"max_storage"`
	} `json:"config"`
	Reserved uint64 `json:"reserved_storage"`
	Storage  uint64 `json:"storage"`
	Meta     struct {
		Leader   string `json:"leader"`
		Size     int    `json:"cluster_size"`
		Pending  int    `json:"pending"`
		Replicas []struct {
			Name    string `json:"name"`
			Current bool   `json:"current"`
			Offline bool   `json:"offline"`
			Lag     uint64 `json:"lag"`
		} `json:"replicas"`
	} `json:"meta_cluster"`
}

func preflightCapacity(ctx context.Context) error {
	var pods struct {
		Items []struct {
			Metadata struct{ Name string }
			Status   struct {
				PodIP      string
				Conditions []struct{ Type, Status string }
			}
		}
	}
	if err := kube("pods", "", &pods); err != nil {
		return err
	}
	var members []memberCapacity
	client := &http.Client{Timeout: 5 * time.Second}
	for _, pod := range pods.Items {
		if pod.Metadata.Name != "nats-0" && pod.Metadata.Name != "nats-1" && pod.Metadata.Name != "nats-2" {
			continue
		}
		ready := false
		for _, c := range pod.Status.Conditions {
			if c.Type == "Ready" && c.Status == "True" {
				ready = true
			}
		}
		if !ready || pod.Status.PodIP == "" {
			return errors.New("NATS member is not ready before bootstrap")
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+pod.Status.PodIP+":8222/jsz", nil)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		var member memberCapacity
		err = json.NewDecoder(response.Body).Decode(&member)
		_ = response.Body.Close()
		if err != nil {
			return err
		}
		if response.StatusCode != http.StatusOK {
			return errors.New("NATS quota observation failed")
		}
		member.Name = pod.Metadata.Name
		members = append(members, member)
	}
	return validateCapacity(members)
}

func validateCapacity(members []memberCapacity) error {
	if len(members) != 3 {
		return errors.New("bootstrap requires three observed NATS members")
	}
	leader := members[0].Meta.Leader
	seen := map[string]bool{}
	leaderChecked := false
	for _, member := range members {
		if seen[member.Name] || (member.Name != "nats-0" && member.Name != "nats-1" && member.Name != "nats-2") {
			return errors.New("NATS member identity is duplicated or unexpected")
		}
		seen[member.Name] = true
		used := max(member.Storage, member.Reserved)
		if used > member.Config.MaxStorage || member.Config.MaxStorage-used < uint64(eventstream.StreamConfig().MaxBytes) {
			return errors.New("NATS member has insufficient unreserved file quota for release stream")
		}
		if leader == "" || member.Meta.Leader != leader || member.Meta.Size != 3 || member.Meta.Pending != 0 {
			return errors.New("NATS metadata quorum is not stable")
		}
		if member.Name == leader {
			peers := map[string]bool{}
			for _, peer := range member.Meta.Replicas {
				if peer.Name == leader || peers[peer.Name] || !peer.Current || peer.Offline || peer.Lag != 0 {
					return errors.New("NATS metadata replica is not current")
				}
				peers[peer.Name] = true
			}
			for _, expected := range []string{"nats-0", "nats-1", "nats-2"} {
				if expected != leader && !peers[expected] {
					return errors.New("NATS metadata replica is missing")
				}
			}
			leaderChecked = len(peers) == 2
		}
	}
	if !leaderChecked {
		return errors.New("NATS metadata leader was not observed")
	}
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
