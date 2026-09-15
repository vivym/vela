import base64
import copy
import datetime
import hashlib
import importlib.util
import pathlib
import unittest

import yaml
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

spec = importlib.util.spec_from_file_location("webhook_host", pathlib.Path(__file__).with_name("prepare-fleet-webhook-host.py"))
host = importlib.util.module_from_spec(spec)
spec.loader.exec_module(host)


class WebhookHostTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime.datetime.now(datetime.timezone.utc)
        self.ca_key = ec.generate_private_key(ec.SECP256R1())
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-ca")])
        self.ca = (x509.CertificateBuilder().subject_name(subject).issuer_name(subject)
                   .public_key(self.ca_key.public_key()).serial_number(1)
                   .not_valid_before(self.now - datetime.timedelta(days=1))
                   .not_valid_after(self.now + datetime.timedelta(days=365))
                   .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
                   .add_extension(x509.KeyUsage(False, False, False, False, False, True, True, False, False), critical=True)
                   .sign(self.ca_key, hashes.SHA256()))
        self.trust = self.ca.public_bytes(serialization.Encoding.PEM)
        self.name = "vela-fleet-apiserver-client-current-r-123456789abc"
        self.uid = "test-secret-uid"
        self.revision = "sha256:" + "1" * 64
        self.original = {"apiVersion": "apiserver.config.k8s.io/v1", "kind": "AdmissionConfiguration", "plugins": [
            {"name": "PodSecurity", "configuration": {"apiVersion": "pod-security.admission.config.k8s.io/v1beta1",
                "kind": "PodSecurityConfiguration", "defaults": {"enforce": "restricted", "enforce-version": "v1.35"},
                "exemptions": {"usernames": ["legacy-operator"], "runtimeClasses": ["special"], "namespaces": ["infra"]}}},
            {"name": "ImagePolicyWebhook", "path": "/etc/existing/image-policy.yaml"}]}

    def secret(self, identity=host.IDENTITY, usages=None, expires=90, future=False):
        key = ec.generate_private_key(ec.SECP256R1())
        cert = (x509.CertificateBuilder().subject_name(x509.Name([])).issuer_name(self.ca.subject)
                .public_key(key.public_key()).serial_number(x509.random_serial_number())
                .not_valid_before(self.now + datetime.timedelta(days=1) if future else self.now - datetime.timedelta(days=2))
                .not_valid_after(self.now + datetime.timedelta(days=expires))
                .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
                .add_extension(x509.SubjectAlternativeName([x509.UniformResourceIdentifier(identity)]), critical=True)
                .add_extension(x509.ExtendedKeyUsage(usages or [ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False)
                .sign(self.ca_key, hashes.SHA256()))
        encode = lambda b: base64.b64encode(b).decode()
        return {"apiVersion": "v1", "kind": "Secret", "immutable": True,
                "metadata": {"name": self.name, "namespace": "vela-system", "uid": self.uid,
                             "annotations": {"vela.ai/release-revision": self.revision}},
                "data": {"ca.crt": encode(self.trust), "tls.crt": encode(cert.public_bytes(serialization.Encoding.PEM)),
                         "tls.key": encode(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                                             serialization.NoEncryption()))}}

    def verify(self, secret):
        return host.verify_material(secret, self.trust, self.name, self.uid, self.revision,
                                    hashlib.sha256(self.trust).hexdigest(), self.now)

    def test_preserves_every_existing_plugin_and_limits_authentication_to_one_service(self):
        original = copy.deepcopy(self.original)
        pem, material = self.verify(self.secret())
        directory, files = host.render(self.original, material)
        self.assertEqual(self.original, original)
        result = yaml.safe_load(files["admission.yaml"])
        self.assertEqual(result["plugins"][:-1], original["plugins"])
        self.assertEqual(result["plugins"][-1]["configuration"]["kubeConfigFile"], str(directory / "kubeconfig"))
        config = yaml.safe_load(files["kubeconfig"])
        self.assertEqual(config["users"], [{"name": "vela-fleet-admission.vela-system.svc:443", "user": {
            "client-certificate": str(directory / "client.pem"), "client-key": str(directory / "client.pem")}}])
        self.assertFalse(config["contexts"])
        self.assertFalse(config["current-context"])
        self.assertFalse(config["clusters"])
        self.assertNotIn("PRIVATE KEY", files["material.json"].decode())
        self.assertIn(b"PRIVATE KEY", pem)

    def test_mutable_or_replaced_secret_cannot_be_staged(self):
        for field in ["immutable", "uid", "revision", "namespace", "extra-key"]:
            with self.subTest(field=field):
                secret = self.secret()
                if field == "immutable": secret[field] = False
                elif field == "revision": secret["metadata"]["annotations"]["vela.ai/release-revision"] = "sha256:" + "2" * 64
                elif field == "extra-key": secret["data"]["token"] = "dG9rZW4="
                else: secret["metadata"][field] = "unexpected"
                with self.assertRaises(ValueError): self.verify(secret)

    def test_rejects_other_identity_usage_and_invalid_validity_window(self):
        invalid = [self.secret(identity="spiffe://vela.internal/fleet-controller/primary"),
                   self.secret(usages=[ExtendedKeyUsageOID.SERVER_AUTH]),
                   self.secret(usages=[ExtendedKeyUsageOID.CLIENT_AUTH, ExtendedKeyUsageOID.SERVER_AUTH]),
                   self.secret(expires=-1), self.secret(expires=7), self.secret(future=True)]
        for secret in invalid:
            with self.subTest(serial=secret["data"]["tls.crt"][-24:]):
                with self.assertRaises(ValueError): self.verify(secret)

    def test_rejects_unrelated_private_key_and_unpinned_root(self):
        secret = self.secret()
        secret["data"]["tls.key"] = self.secret()["data"]["tls.key"]
        with self.assertRaisesRegex(ValueError, "private key mismatch"): self.verify(secret)
        with self.assertRaisesRegex(ValueError, "CA pin mismatch"):
            host.verify_material(self.secret(), self.trust, self.name, self.uid, self.revision, "0" * 64, self.now)

    def test_existing_webhook_or_duplicate_security_plugin_is_not_overwritten(self):
        _, material = self.verify(self.secret())
        for extra in [{"name": "ValidatingAdmissionWebhook", "path": "/etc/another-webhook-client.yaml"},
                      copy.deepcopy(self.original["plugins"][0])]:
            original = copy.deepcopy(self.original)
            original["plugins"].append(extra)
            with self.assertRaises(ValueError): host.render(original, material)
        original = copy.deepcopy(self.original)
        original["plugins"] = []
        with self.assertRaises(ValueError): host.render(original, material)

    def test_uses_active_static_pod_path_and_rejects_ambiguous_configuration(self):
        for args in [["--admission-control-config-file=/custom/admission.yaml"],
                     ["--admission-control-config-file", "/custom/admission.yaml"]]:
            self.assertEqual(host.admission_path({"spec": {"containers": [{"command": ["kube-apiserver"], "args": args}]}}),
                             pathlib.Path("/custom/admission.yaml"))
        for args in [[], ["--admission-control-config-file=/a", "--admission-control-config-file=/b"]]:
            with self.assertRaises(ValueError): host.admission_path({"spec": {"containers": [{"args": args}]}})


if __name__ == "__main__":
    unittest.main()
