#!/usr/bin/env python3
"""Stage immutable Fleet webhook client files; never activate or restart RKE2.

Requires PyYAML and cryptography. Secret values stay in root-only files and are
never included in receipts. The existing admission plugin configuration is read
from the running static Pod specification, not from an assumed default path.
"""
import argparse
import base64
import copy
import datetime
import hashlib
import json
import os
import pathlib
import re
import shutil
import stat
import tempfile

import yaml
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID

SERVICE = "vela-fleet-admission.vela-system.svc:443"
IDENTITY = "spiffe://vela.internal/kube-apiserver/admission"
BASE = pathlib.Path("/var/lib/rancher/rke2/server/vela-fleet-admission/candidates")


def require(value, message):
    if not value:
        raise ValueError(message)


def certificate_time(cert, field):
    value = getattr(cert, field + "_utc", None)
    return value if value is not None else getattr(cert, field).replace(tzinfo=datetime.timezone.utc)


def verify_material(secret, trust_pem, name, uid, revision, trust_sha256, now=None):
    require(hashlib.sha256(trust_pem).hexdigest() == trust_sha256, "client CA pin mismatch")
    meta = secret.get("metadata", {})
    require(secret.get("kind") == "Secret" and secret.get("apiVersion") == "v1"
            and secret.get("immutable") is True, "immutable v1 Secret required")
    require(meta.get("namespace") == "vela-system" and meta.get("name") == name
            and meta.get("uid") == uid and bool(uid), "Secret identity mismatch")
    require(re.fullmatch(r"vela-fleet-apiserver-client-current-r-[0-9a-f]{12}", name), "unexpected Secret name")
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", revision)
            and meta.get("annotations", {}).get("vela.ai/release-revision") == revision, "Secret revision mismatch")
    data = secret.get("data", {})
    require(set(data) == {"ca.crt", "tls.crt", "tls.key"}, "unexpected Secret keys")
    decoded = {key: base64.b64decode(value, validate=True) for key, value in data.items()}
    require(all(0 < len(value) <= 65536 for value in decoded.values()), "invalid TLS material size")
    ca = x509.load_pem_x509_certificate(trust_pem)
    require(ca.fingerprint(hashes.SHA256()) == x509.load_pem_x509_certificate(decoded["ca.crt"]).fingerprint(hashes.SHA256()),
            "Secret CA differs from pinned admission client CA")
    cert = x509.load_pem_x509_certificate(decoded["tls.crt"])
    key = serialization.load_pem_private_key(decoded["tls.key"], password=None)
    public_format = (serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    require(key.public_key().public_bytes(*public_format) == cert.public_key().public_bytes(*public_format), "private key mismatch")
    require(ca.extensions.get_extension_for_class(x509.BasicConstraints).value.ca, "trust certificate is not a CA")
    require(ca.extensions.get_extension_for_class(x509.KeyUsage).value.key_cert_sign, "CA cannot sign certificates")
    require(not cert.extensions.get_extension_for_class(x509.BasicConstraints).value.ca, "client certificate is a CA")
    require(set(cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value) == {ExtendedKeyUsageOID.CLIENT_AUTH},
            "exclusive clientAuth usage required")
    sans = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    require(list(sans) == [x509.UniformResourceIdentifier(IDENTITY)], "admission SPIFFE identity mismatch")
    require(cert.issuer == ca.subject and isinstance(ca.public_key(), ec.EllipticCurvePublicKey), "unexpected issuer")
    ca.public_key().verify(cert.signature, cert.tbs_certificate_bytes, ec.ECDSA(cert.signature_hash_algorithm))
    now = now or datetime.datetime.now(datetime.timezone.utc)
    for item in [cert, ca]:
        require(certificate_time(item, "not_valid_before") <= now
                and certificate_time(item, "not_valid_after") > now + datetime.timedelta(days=14),
                "certificate is not current or has insufficient validity")
    return decoded["tls.crt"].rstrip() + b"\n" + decoded["tls.key"].rstrip() + b"\n", {
        "secret_name": name, "secret_uid": uid, "secret_revision": revision,
        "certificate_sha256": cert.fingerprint(hashes.SHA256()).hex(),
        "client_ca_file_sha256": trust_sha256,
        "not_after": certificate_time(cert, "not_valid_after").isoformat(), "identity": IDENTITY,
    }


def render(admission, material, base=BASE):
    require(admission.get("apiVersion") == "apiserver.config.k8s.io/v1"
            and admission.get("kind") == "AdmissionConfiguration", "unsupported admission configuration")
    plugins = admission.get("plugins", [])
    require(isinstance(plugins, list) and all(isinstance(p, dict) and isinstance(p.get("name"), str) for p in plugins),
            "invalid admission plugins")
    names = [p["name"] for p in plugins]
    require(len(names) == len(set(names)) and "PodSecurity" in names, "PodSecurity or unique plugin names missing")
    require("ValidatingAdmissionWebhook" not in names, "existing webhook client configuration requires explicit reconciliation")
    payload = json.dumps({"admission": admission, "material": material}, sort_keys=True).encode()
    directory = base / hashlib.sha256(payload).hexdigest()[:24]
    certificate_path = str(directory / "client.pem")
    kubeconfig = {"apiVersion": "v1", "kind": "Config", "clusters": [], "contexts": [], "current-context": "",
                  "users": [{"name": SERVICE, "user": {"client-certificate": certificate_path, "client-key": certificate_path}}]}
    candidate = copy.deepcopy(admission)
    candidate["plugins"].append({"name": "ValidatingAdmissionWebhook", "configuration": {
        "apiVersion": "apiserver.config.k8s.io/v1", "kind": "WebhookAdmissionConfiguration",
        "kubeConfigFile": str(directory / "kubeconfig"),
    }})
    files = {"admission.yaml": yaml.safe_dump(candidate, sort_keys=False).encode(),
             "kubeconfig": yaml.safe_dump(kubeconfig, sort_keys=False).encode(),
             "rke2-drop-in.candidate.yaml": yaml.safe_dump({"pod-security-admission-config-file": str(directory / "admission.yaml")}).encode(),
             "material.json": (json.dumps(material, indent=2) + "\n").encode()}
    return directory, files


def read_owned_file(path, private=False):
    require(path.is_absolute(), "absolute file path required")
    require(not any(p.is_symlink() for p in [path, *path.parents]), "symlink path refused")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and info.st_size <= 1048576, "invalid root-owned input")
        require(info.st_mode & (0o077 if private else 0o022) == 0, "unsafe input permissions")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            return stream.read(1048577)
    finally:
        os.close(fd)


def admission_path(pod):
    containers = pod["spec"]["containers"]
    require(len(containers) == 1, "unexpected API Server container set")
    args = containers[0].get("command", []) + containers[0].get("args", [])
    paths = []
    for index, value in enumerate(args):
        if value.startswith("--admission-control-config-file="):
            paths.append(value.split("=", 1)[1])
        elif value == "--admission-control-config-file" and index + 1 < len(args):
            paths.append(args[index + 1])
    require(len(paths) == 1, "one active admission configuration path required")
    return pathlib.Path(paths[0])


def stage(directory, files):
    base = directory.parent
    require(not directory.is_symlink(), "symlink candidate refused")
    require(not any(p.is_symlink() for p in [base, *base.parents]), "symlink output parent refused")
    for parent in [base, *base.parents]:
        if parent.exists():
            info = parent.stat()
            require(info.st_uid == 0 and info.st_mode & 0o022 == 0, "unsafe output parent")
    base.mkdir(mode=0o700, parents=True, exist_ok=True)
    if directory.exists():
        require(not directory.is_symlink() and directory.stat().st_uid == 0
                and stat.S_IMODE(directory.stat().st_mode) == 0o700, "unsafe existing candidate")
        require({p.name for p in directory.iterdir()} == set(files), "candidate file set differs")
        for name, body in files.items():
            require(read_owned_file(directory / name, private=True) == body
                    and stat.S_IMODE((directory / name).stat().st_mode) == 0o400, "candidate content or mode differs")
        return False
    temporary = pathlib.Path(tempfile.mkdtemp(prefix=".prepare-", dir=base))
    try:
        for name, body in files.items():
            with (temporary / name).open("xb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            (temporary / name).chmod(0o400)
        os.rename(temporary, directory)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--secret-json", type=pathlib.Path, required=True)
    parser.add_argument("--secret-name", required=True)
    parser.add_argument("--secret-uid", required=True)
    parser.add_argument("--secret-revision", required=True)
    parser.add_argument("--client-ca", type=pathlib.Path, required=True)
    parser.add_argument("--client-ca-sha256", required=True)
    args = parser.parse_args()
    require(os.geteuid() == 0, "root is required for host candidate preparation")
    os.umask(0o077)
    pod = yaml.safe_load(read_owned_file(pathlib.Path("/var/lib/rancher/rke2/agent/pod-manifests/kube-apiserver.yaml")))
    original_path = admission_path(pod)
    original_bytes = read_owned_file(original_path)
    admission = yaml.safe_load(original_bytes)
    material_bytes, material = verify_material(json.loads(read_owned_file(args.secret_json, private=True)),
        read_owned_file(args.client_ca), args.secret_name, args.secret_uid, args.secret_revision, args.client_ca_sha256)
    directory, files = render(admission, material)
    files["client.pem"] = material_bytes
    created = stage(directory, files)
    require(read_owned_file(original_path) == original_bytes, "active admission configuration changed concurrently")
    print(json.dumps({"at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "passed": True,
        "activated": False, "created": created, "directory": str(directory), "source_admission": str(original_path),
        "source_admission_sha256": hashlib.sha256(original_bytes).hexdigest(), "material": material,
        "files": [{"name": name, "mode": "0400", "sha256": hashlib.sha256(body).hexdigest()} for name, body in files.items()]}))


if __name__ == "__main__":
    main()
