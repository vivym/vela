// A bounded live NATS recovery probe, isolated from business stream subjects.
// Credentials are read from existing Kubernetes Secrets into private temporary
// files. Output contains only test message hashes, stream state and peer health.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type state struct {
	Stream     string    `json:"stream"`
	Subject    string    `json:"subject"`
	Consumer   string    `json:"consumer"`
	Created    time.Time `json:"created"`
	PayloadSHA string    `json:"payload_sha256"`
	Sequence   uint64    `json:"sequence"`
}

func kube(kind, name string, dst any) error {
	args := []string{"-n", "vela-system", "get", kind}
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
	cleanup := func() { os.RemoveAll(dir) }
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
func payload(s state) []byte { return []byte("Vela isolated live recovery marker / " + s.Stream) }
func save(path string, s state) error {
	b, e := json.MarshalIndent(s, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(path, b, 0600)
}
func run() error {
	if len(os.Args) != 3 {
		return errors.New("usage: nats-live-recovery prepare|verify|redelivery|cleanup STATE_PATH")
	}
	mode, path := os.Args[1], os.Args[2]
	var s state
	if mode == "prepare" {
		if _, e := os.Stat(path); !os.IsNotExist(e) {
			return errors.New("refuse existing state path")
		}
		// Unpredictable names identify exact resources owned by this run.
		id := fmt.Sprintf("%x", sha256.Sum256([]byte(time.Now().String()+path)))[:16]
		s = state{Stream: "VELA_DRILL_" + id, Subject: "vela.events.drill." + id, Consumer: "DRILL", Created: time.Now().UTC()}
		sum := sha256.Sum256(payload(s))
		s.PayloadSHA = hex.EncodeToString(sum[:])
	} else {
		b, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		if e = json.Unmarshal(b, &s); e != nil {
			return e
		}
		if !strings.HasPrefix(s.Stream, "VELA_DRILL_") || s.Subject != "vela.events.drill."+strings.TrimPrefix(s.Stream, "VELA_DRILL_") || s.Consumer != "DRILL" {
			return errors.New("invalid recovery state ownership")
		}
	}
	nc, closeAdmin, e := connect("bootstrap")
	if e != nil {
		return e
	}
	defer closeAdmin()
	js, e := jetstream.New(nc)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if mode == "prepare" {
		// This probe intentionally runs only before business bootstrap; never claim
		// an overlapping subject in an active VELA_EVENTS stream.
		list := js.ListStreams(ctx)
		n := 0
		for range list.Info() {
			n++
		}
		if list.Err() != nil {
			return list.Err()
		}
		if n != 0 {
			return errors.New("existing streams: choose an isolated account for recovery drill")
		}
		if e = save(path, s); e != nil {
			return e
		}
		stream, e := js.CreateStream(ctx, jetstream.StreamConfig{Name: s.Stream, Subjects: []string{s.Subject}, Storage: jetstream.FileStorage, Replicas: 3, MaxBytes: 1 << 20, MaxMsgs: 100, MaxAge: time.Hour, Duplicates: 10 * time.Minute, Metadata: map[string]string{"vela.drill": s.Stream}})
		if e != nil {
			return e
		}
		consumer, e := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Name: s.Consumer, Durable: s.Consumer, AckPolicy: jetstream.AckExplicitPolicy, AckWait: 5 * time.Second, Replicas: 3, FilterSubject: s.Subject})
		if e != nil {
			return e
		}
		publisher, closePub, e := connect("outbox")
		if e != nil {
			return e
		}
		defer closePub()
		pubJS, e := jetstream.New(publisher)
		if e != nil {
			return e
		}
		ack, e := pubJS.Publish(ctx, s.Subject, payload(s), jetstream.WithMsgID(s.Stream))
		if e != nil {
			return e
		}
		if ack.Stream != s.Stream || ack.Sequence != 1 {
			return errors.New("unexpected PubAck")
		}
		s.Sequence = ack.Sequence
		if e = save(path, s); e != nil {
			return e
		}
		msg, e := consumer.Next(jetstream.FetchMaxWait(8 * time.Second))
		if e != nil {
			return e
		}
		meta, e := msg.Metadata()
		if e != nil {
			return e
		}
		if meta.Sequence.Stream != s.Sequence {
			return errors.New("unexpected first delivery")
		}
		// Leave unacknowledged deliberately. The admin identity has no business Ack
		// authority; durable pending state must survive member replacements.
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"phase": "prepared", "state": s, "first_delivery": meta.NumDelivered, "pending_unacked": true})
	}
	stream, e := js.Stream(ctx, s.Stream)
	if e != nil {
		return e
	}
	info, e := stream.Info(ctx)
	if e != nil {
		return e
	}
	if info.Config.Metadata["vela.drill"] != s.Stream || !info.Created.Equal(s.Created) && info.Created.Before(s.Created) {
		return errors.New("stream ownership mismatch")
	}
	if mode == "cleanup" {
		if e = js.DeleteStream(ctx, s.Stream); e != nil {
			return e
		}
		_, e = js.Stream(ctx, s.Stream)
		if !errors.Is(e, jetstream.ErrStreamNotFound) {
			return errors.New("stream deletion not confirmed")
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"phase": "cleaned", "stream": s.Stream})
	}
	if info.Config.Storage != jetstream.FileStorage || info.Config.Replicas != 3 || info.State.Msgs != 1 || info.Cluster == nil || len(info.Cluster.Replicas) != 2 {
		return errors.New("stream quorum or stored count differs")
	}
	for _, replica := range info.Cluster.Replicas {
		if !replica.Current || replica.Offline || replica.Lag != 0 {
			return errors.New("stream replica not current")
		}
	}
	raw, e := stream.GetMsg(ctx, s.Sequence)
	if e != nil {
		return e
	}
	sum := sha256.Sum256(raw.Data)
	if hex.EncodeToString(sum[:]) != s.PayloadSHA {
		return errors.New("persisted message hash differs")
	}
	consumer, e := stream.Consumer(ctx, s.Consumer)
	if e != nil {
		return e
	}
	ci, e := consumer.Info(ctx)
	if e != nil {
		return e
	}
	if ci.Config.Replicas != 3 || ci.NumAckPending != 1 {
		return errors.New("durable unacked consumer state differs")
	}
	result := map[string]any{"phase": mode, "stream": s.Stream, "message_sha256": s.PayloadSHA, "message_count": info.State.Msgs, "cluster": info.Cluster, "pending_unacked": ci.NumAckPending, "consumer_cluster": ci.Cluster, "observed_at": time.Now().UTC()}
	if mode == "redelivery" {
		msg, e := consumer.Next(jetstream.FetchMaxWait(8 * time.Second))
		if e != nil {
			return e
		}
		meta, e := msg.Metadata()
		if e != nil {
			return e
		}
		got := sha256.Sum256(msg.Data())
		if hex.EncodeToString(got[:]) != s.PayloadSHA || meta.Sequence.Stream != s.Sequence || meta.NumDelivered < 2 {
			return errors.New("durable redelivery was not verified")
		}
		result["delivery_count"] = meta.NumDelivered
		result["stream_sequence"] = meta.Sequence.Stream
	} else if mode != "verify" {
		return errors.New("unsupported mode")
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
