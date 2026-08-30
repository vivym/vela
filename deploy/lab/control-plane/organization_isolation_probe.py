#!/usr/bin/env python3

import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone


MAX_API_BYTES = 1 << 20
MAX_ARTIFACT_BYTES = 64 << 20
MAX_SOURCE_BYTES = 1 << 20
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class ProbeError(Exception):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


OPENER = urllib.request.build_opener(NoRedirect())


def required_environment(name):
    value = os.environ.get(name, "")
    if not value:
        raise ProbeError(f"{name} is required")
    return value


def file_sha256(path):
    digest = hashlib.sha256()
    total = 0
    with open(path, "rb") as source:
        while True:
            chunk = source.read(65536)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_SOURCE_BYTES:
                raise ProbeError("probe source exceeded the source bound")
            digest.update(chunk)
    return digest.hexdigest()


def read_bounded(response, limit):
    body = response.read(limit + 1)
    if len(body) > limit:
        raise ProbeError("HTTP response exceeded the probe bound")
    return body


def request_bytes(url, method, bearer=None, limit=MAX_API_BYTES):
    request = urllib.request.Request(url, method=method)
    if bearer is not None:
        request.add_header("Authorization", "Bearer " + bearer)
    try:
        with OPENER.open(request, timeout=30) as response:
            return response.status, read_bounded(response, limit)
    except urllib.error.HTTPError as error:
        try:
            body = read_bounded(error, limit)
        finally:
            error.close()
        return error.code, body
    except (OSError, urllib.error.URLError) as error:
        raise ProbeError("HTTP request failed") from error


def request_json(url, expected_status, bearer):
    status, body = request_bytes(url, "GET", bearer)
    if status != expected_status:
        raise ProbeError(f"API request returned HTTP {status}, expected {expected_status}")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProbeError("API response was not one JSON document") from error
    if not isinstance(value, dict):
        raise ProbeError("API response was not a JSON object")
    return value


def expect_hidden(url, bearer):
    status, _ = request_bytes(url, "GET", bearer)
    if status != 404:
        raise ProbeError(f"cross-scope API request returned HTTP {status}, expected 404")


def tampered_path(url):
    parsed = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit(parsed._replace(path=parsed.path + ".cross-scope"))


def tampered_version(url):
    parsed = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    replaced = False
    changed = []
    for name, value in query:
        if name.lower() == "versionid":
            if not value:
                raise ProbeError("signed Artifact version was empty")
            replacement = "0" if value[-1] != "0" else "1"
            changed.append((name, value[:-1] + replacement))
            replaced = True
        else:
            changed.append((name, value))
    if not replaced:
        raise ProbeError("signed Artifact URL omitted versionId")
    encoded = urllib.parse.urlencode(changed)
    return urllib.parse.urlunsplit(parsed._replace(query=encoded))


def verify_signed_artifact(artifact, signed_host):
    required = {
        "artifact_id",
        "kind",
        "size_bytes",
        "sha256",
        "download_url",
        "object_version_id",
    }
    if not required.issubset(artifact):
        raise ProbeError("Artifact response omitted required fields")
    size_bytes = artifact["size_bytes"]
    if (
        isinstance(size_bytes, bool)
        or not isinstance(size_bytes, int)
        or not 0 <= size_bytes <= MAX_ARTIFACT_BYTES
    ):
        raise ProbeError("Artifact size was invalid")
    if (
        not isinstance(artifact["sha256"], str)
        or SHA256_PATTERN.fullmatch(artifact["sha256"]) is None
    ):
        raise ProbeError("Artifact SHA-256 was invalid")
    if (
        not isinstance(artifact["object_version_id"], str)
        or not artifact["object_version_id"]
    ):
        raise ProbeError("Artifact object version was invalid")
    url = artifact["download_url"]
    if not isinstance(url, str):
        raise ProbeError("signed Artifact URL was invalid")
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "http" or parsed.netloc != signed_host or parsed.username is not None:
        raise ProbeError("signed Artifact URL escaped the lab object-store origin")
    if not parsed.path or not parsed.query or parsed.fragment:
        raise ProbeError("signed Artifact URL was incomplete")
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    if query.get("versionId") != [artifact["object_version_id"]]:
        raise ProbeError("signed Artifact URL was not bound to the exact object version")

    status, body = request_bytes(url, "GET", limit=MAX_ARTIFACT_BYTES)
    if status != 200:
        raise ProbeError(f"authorized signed Artifact GET returned HTTP {status}")
    if len(body) != size_bytes or hashlib.sha256(body).hexdigest() != artifact["sha256"]:
        raise ProbeError("signed Artifact bytes did not match Visible Completion metadata")

    negative_urls = (
        ("method", url, "HEAD"),
        ("path", tampered_path(url), "GET"),
        ("version", tampered_version(url), "GET"),
    )
    for boundary, negative_url, method in negative_urls:
        status, _ = request_bytes(negative_url, method, limit=MAX_ARTIFACT_BYTES)
        if status != 403:
            raise ProbeError(
                f"signed Artifact {boundary} probe returned HTTP {status}, expected 403"
            )
    return 3


def run_probe():
    base_url = required_environment("VELA_PROBE_BASE_URL").rstrip("/")
    own_project = required_environment("VELA_PROBE_OWN_PROJECT_ID")
    same_org_project = required_environment("VELA_PROBE_SAME_ORG_PROJECT_ID")
    other_org_project = required_environment("VELA_PROBE_OTHER_ORG_PROJECT_ID")
    job_id = required_environment("VELA_PROBE_JOB_ID")
    signed_host = required_environment("VELA_PROBE_SIGNED_HOST")
    credential_file = required_environment("VELA_PROBE_CREDENTIAL_FILE")
    expected_source_sha256 = required_environment("VELA_PROBE_SOURCE_SHA256")
    if SHA256_PATTERN.fullmatch(expected_source_sha256) is None:
        raise ProbeError("VELA_PROBE_SOURCE_SHA256 was invalid")
    observed_source_sha256 = file_sha256(__file__)
    if observed_source_sha256 != expected_source_sha256:
        raise ProbeError("executed probe source did not match the expected SHA-256")

    parsed_base = urllib.parse.urlsplit(base_url)
    if (
        parsed_base.scheme != "http"
        or not parsed_base.netloc
        or parsed_base.username is not None
        or parsed_base.path
        or parsed_base.query
        or parsed_base.fragment
    ):
        raise ProbeError("VELA_PROBE_BASE_URL must be one plain HTTP origin")
    with open(credential_file, "r", encoding="utf-8") as credential_input:
        bearer = credential_input.read(1025).strip()
    if not bearer.startswith("vla_") or len(bearer) > 1024:
        raise ProbeError("probe bearer credential was invalid")

    own_job_url = f"{base_url}/v1/projects/{own_project}/jobs/{job_id}"
    own_artifacts_url = own_job_url + "/artifacts"
    job = request_json(own_job_url, 200, bearer)
    if job.get("job_id") != job_id or job.get("project_id") != own_project:
        raise ProbeError("authorized Job response identity was invalid")

    negative_probe_count = 0
    for project in (same_org_project, other_org_project):
        hidden_job = f"{base_url}/v1/projects/{project}/jobs/{job_id}"
        expect_hidden(hidden_job, bearer)
        expect_hidden(hidden_job + "/artifacts", bearer)
        negative_probe_count += 2

    artifact_set = request_json(own_artifacts_url, 200, bearer)
    artifacts = artifact_set.get("artifacts")
    if artifact_set.get("job_id") != job_id or not isinstance(artifacts, list) or len(artifacts) != 2:
        raise ProbeError("authorized ArtifactSet response was invalid")
    kinds = []
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            raise ProbeError("ArtifactSet contained a non-object Artifact")
        kinds.append(artifact.get("kind"))
        negative_probe_count += verify_signed_artifact(artifact, signed_host)
    if sorted(kinds) != ["THUMBNAIL", "VIDEO"]:
        raise ProbeError("ArtifactSet kinds were invalid")

    return {
        "schema": "vela-lab-organization-isolation-http-probe-v1",
        "status": "LAB_REHEARSAL_PASS",
        "evidence_boundary": "NON_PRODUCTION_MOCK_REHEARSAL",
        "production_gates": "0/9",
        "captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "job_id": job_id,
        "probe_sha256": observed_source_sha256,
        "authorized_artifact_count": len(artifacts),
        "authorized_signed_get_count": len(artifacts),
        "negative_probe_count": negative_probe_count,
        "unexpected_allow_count": 0,
        "cross_project_hidden": True,
        "cross_organization_hidden": True,
        "signed_url_method_bound": True,
        "signed_url_path_bound": True,
        "signed_url_version_bound": True,
    }


def main():
    try:
        receipt = run_probe()
    except ProbeError as error:
        print(f"organization isolation probe failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
