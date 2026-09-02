package fleetcontroller

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const stageWorkerMemberPort int32 = 7444

var memberPKISuffixes = []string{
	"client.crt",
	"client.key",
	"server-ca.crt",
	"server.crt",
	"server.key",
	"client-ca.crt",
}

type stageWorkerMemberEndpoint struct {
	WorkerMemberID string `json:"worker_member_id"`
	MemberEpoch    int64  `json:"member_epoch"`
	IdentityDigest string `json:"identity_digest"`
	Address        string `json:"address,omitempty"`
	ServerName     string `json:"server_name,omitempty"`
}

func mustEncodeStageWorkerMembers(
	bundle WorkerBundleActuation,
	worker WorkerInstanceActuation,
) string {
	members := append([]WorkerMemberActuation(nil), worker.Members...)
	sort.Slice(members, func(left, right int) bool {
		return members[left].ID.String() < members[right].ID.String()
	})
	endpoints := make([]stageWorkerMemberEndpoint, 0, len(members))
	for _, member := range members {
		endpoint := stageWorkerMemberEndpoint{
			WorkerMemberID: member.ID.String(),
			MemberEpoch:    member.MemberEpoch,
			IdentityDigest: member.IdentityDigest,
		}
		if len(members) > 1 {
			endpoint.ServerName = workerInstanceMemberServerName(bundle.Namespace, worker.ID, member.Key)
			endpoint.Address = fmt.Sprintf("%s:%d", endpoint.ServerName, stageWorkerMemberPort)
		}
		endpoints = append(endpoints, endpoint)
	}
	encoded, err := json.Marshal(endpoints)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func materializeWorkerInstanceMemberServices(bundle WorkerBundleActuation) []corev1.Service {
	services := make([]corev1.Service, 0)
	for _, worker := range bundle.WorkerInstances {
		if len(worker.Members) < 2 {
			continue
		}
		for _, member := range worker.Members {
			labels := workerMemberResourceLabels(bundle, worker, member)
			services = append(services, corev1.Service{
				TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
				ObjectMeta: metav1.ObjectMeta{
					Namespace: bundle.Namespace,
					Name:      workerInstanceMemberServiceName(worker.ID, member.Key),
					Labels:    labels,
					Annotations: map[string]string{
						"vela.ai/fleet-controller-owned": "true",
						actuationRevisionAnnotation:      bundle.RevisionDigest,
					},
					Finalizers: []string{protectionFinalizer},
				},
				Spec: corev1.ServiceSpec{
					Type:                     corev1.ServiceTypeClusterIP,
					PublishNotReadyAddresses: true,
					SessionAffinity:          corev1.ServiceAffinityNone,
					InternalTrafficPolicy:    valuePointer(corev1.ServiceInternalTrafficPolicyCluster),
					Selector: map[string]string{
						workerInstanceIDLabel: worker.ID.String(),
						workerMemberIDLabel:   member.ID.String(),
					},
					Ports: []corev1.ServicePort{{
						Name: "grpc-member", Protocol: corev1.ProtocolTCP, Port: stageWorkerMemberPort,
						TargetPort: intstr.FromInt32(stageWorkerMemberPort),
					}},
				},
			})
		}
	}
	return services
}

func materializeWorkerInstanceMemberSecrets(
	bundle WorkerBundleActuation,
	source corev1.Secret,
) ([]corev1.Secret, error) {
	if source.Namespace != bundle.Namespace || source.Name != bundle.StageWorkerMemberPKISecret ||
		source.Immutable == nil || !*source.Immutable || source.Type != corev1.SecretTypeOpaque {
		return nil, errors.New("stage worker member PKI source Secret is invalid or mutable")
	}
	secrets := make([]corev1.Secret, 0)
	for _, worker := range bundle.WorkerInstances {
		if len(worker.Members) < 2 {
			continue
		}
		for _, member := range worker.Members {
			serviceDNS := workerInstanceMemberServerName(bundle.Namespace, worker.ID, member.Key)
			data, err := validatedMemberPKIData(source.Data, member, serviceDNS)
			if err != nil {
				return nil, fmt.Errorf("validate WorkerMember %s PKI: %w", member.ID, err)
			}
			immutable := true
			secrets = append(secrets, corev1.Secret{
				TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
				ObjectMeta: metav1.ObjectMeta{
					Namespace: bundle.Namespace,
					Name:      workerInstanceMemberSecretName(worker.ID, member.Key),
					Labels:    workerMemberResourceLabels(bundle, worker, member),
					Annotations: map[string]string{
						"vela.ai/fleet-controller-owned": "true",
						actuationRevisionAnnotation:      bundle.RevisionDigest,
					},
					Finalizers: []string{protectionFinalizer},
				},
				Immutable: &immutable,
				Type:      corev1.SecretTypeOpaque,
				Data:      data,
			})
		}
	}
	return secrets, nil
}

func validatedMemberPKIData(
	source map[string][]byte,
	member WorkerMemberActuation,
	serverName string,
) (map[string][]byte, error) {
	if len(source) == 0 {
		return nil, errors.New("member PKI source data is empty")
	}
	prefix := member.ID.String() + "."
	values := make(map[string][]byte, len(memberPKISuffixes))
	for _, suffix := range memberPKISuffixes {
		value := source[prefix+suffix]
		if len(value) == 0 || len(value) > 4<<20 {
			return nil, fmt.Errorf("member PKI key %s is missing or too large", suffix)
		}
		values[suffix] = bytes.Clone(value)
	}
	clientPair, err := tls.X509KeyPair(values["client.crt"], values["client.key"])
	if err != nil {
		return nil, errors.New("member client certificate and key do not match")
	}
	serverPair, err := tls.X509KeyPair(values["server.crt"], values["server.key"])
	if err != nil {
		return nil, errors.New("member server certificate and key do not match")
	}
	if err := verifyMemberLeaf(
		clientPair, values["client-ca.crt"], member.IdentityDigest, "", x509.ExtKeyUsageClientAuth,
	); err != nil {
		return nil, fmt.Errorf("verify member client certificate: %w", err)
	}
	if err := verifyMemberLeaf(
		serverPair, values["server-ca.crt"], member.IdentityDigest, serverName, x509.ExtKeyUsageServerAuth,
	); err != nil {
		return nil, fmt.Errorf("verify member server certificate: %w", err)
	}
	return map[string][]byte{
		"client.crt":    values["client.crt"],
		"client.key":    values["client.key"],
		"server-ca.crt": values["server-ca.crt"],
		"server.crt":    values["server.crt"],
		"server.key":    values["server.key"],
		"client-ca.crt": values["client-ca.crt"],
	}, nil
}

func verifyMemberLeaf(
	pair tls.Certificate,
	caPEM []byte,
	expectedDigest string,
	serverName string,
	usage x509.ExtKeyUsage,
) error {
	if len(pair.Certificate) == 0 {
		return errors.New("member certificate chain is empty")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil || len(leaf.URIs) != 1 || !validMemberIdentityURI(leaf.URIs[0]) {
		return errors.New("member certificate has no unique canonical SPIFFE identity")
	}
	digest := sha256.Sum256([]byte(leaf.URIs[0].String()))
	if hex.EncodeToString(digest[:]) != expectedDigest {
		return errors.New("member certificate identity digest does not match actuation")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return errors.New("member certificate CA contains no certificate")
	}
	intermediates := x509.NewCertPool()
	for _, encoded := range pair.Certificate[1:] {
		certificate, parseErr := x509.ParseCertificate(encoded)
		if parseErr != nil {
			return errors.New("member certificate chain contains an invalid intermediate")
		}
		intermediates.AddCert(certificate)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		DNSName: serverName, Roots: roots, Intermediates: intermediates,
		KeyUsages: []x509.ExtKeyUsage{usage},
	}); err != nil {
		return errors.New("member certificate chain or usage is invalid")
	}
	return nil
}

func validMemberIdentityURI(identity *url.URL) bool {
	if identity == nil {
		return false
	}
	value := identity.String()
	return identity.Scheme == "spiffe" && identity.Host != "" && identity.User == nil &&
		identity.Path != "" && identity.Opaque == "" && identity.RawQuery == "" &&
		identity.Fragment == "" && len(value) <= 500 && strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, '\x00')
}

func workerMemberResourceLabels(
	bundle WorkerBundleActuation,
	worker WorkerInstanceActuation,
	member WorkerMemberActuation,
) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":         "vela-worker-member",
		"app.kubernetes.io/part-of":      "vela",
		"vela.ai/fleet-controller-owned": "true",
		protectedLabel:                   "true",
		workerInstanceIDLabel:            worker.ID.String(),
		workerInstanceEpochLabel:         fmt.Sprintf("%d", worker.InstanceEpoch),
		workerMemberIDLabel:              member.ID.String(),
		workerMemberKeyLabel:             member.Key,
		workerBundleIDLabel:              bundle.WorkerBundleID.String(),
		residencyPlanRevisionLabel:       bundle.PlanRevisionID.String(),
	}
}

func workerInstanceMemberServiceName(workerID uuid.UUID, memberKey string) string {
	return workerInstancePodName(workerID, memberKey) + "-member"
}

func workerInstanceMemberSecretName(workerID uuid.UUID, memberKey string) string {
	return workerInstancePodName(workerID, memberKey) + "-member-tls"
}

func workerInstanceMemberServerName(namespace string, workerID uuid.UUID, memberKey string) string {
	return workerInstanceMemberServiceName(workerID, memberKey) + "." + namespace + ".svc"
}

func workerInstanceMemberServiceMatches(live, desired corev1.Service) bool {
	normalize := func(service corev1.Service) corev1.Service {
		copy := *service.DeepCopy()
		copy.ResourceVersion = ""
		copy.Generation = 0
		copy.UID = ""
		copy.CreationTimestamp = metav1.Time{}
		copy.ManagedFields = nil
		copy.Status = corev1.ServiceStatus{}
		copy.Spec.ClusterIP = ""
		copy.Spec.ClusterIPs = nil
		copy.Spec.IPFamilies = nil
		copy.Spec.IPFamilyPolicy = nil
		copy.Spec.HealthCheckNodePort = 0
		return copy
	}
	return reflect.DeepEqual(normalize(live), normalize(desired))
}

func workerInstanceMemberSecretMatches(live, desired corev1.Secret) bool {
	normalize := func(secret corev1.Secret) corev1.Secret {
		copy := *secret.DeepCopy()
		copy.ResourceVersion = ""
		copy.Generation = 0
		copy.UID = ""
		copy.CreationTimestamp = metav1.Time{}
		copy.ManagedFields = nil
		copy.StringData = nil
		return copy
	}
	return reflect.DeepEqual(normalize(live), normalize(desired))
}
