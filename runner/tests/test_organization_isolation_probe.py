import hashlib
import importlib.util
from email.message import Message
from pathlib import Path

import pytest

PROBE_PATH = (
    Path(__file__).resolve().parents[2]
    / "deploy"
    / "lab"
    / "control-plane"
    / "organization_isolation_probe.py"
)
SPEC = importlib.util.spec_from_file_location("organization_isolation_probe", PROBE_PATH)
assert SPEC is not None and SPEC.loader is not None
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


def test_verify_signed_artifact_returns_complete_negative_ledger(monkeypatch):
    body = b"synthetic-artifact"
    artifact = {
        "artifact_id": "85000000-0000-0000-0000-000000000131",
        "kind": "VIDEO",
        "size_bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
        "download_url": "http://minio.example/object?versionId=version-1&signature=fixed",
        "object_version_id": "version-1",
    }

    def request_bytes(url, method, bearer=None, limit=probe.MAX_API_BYTES):
        del bearer, limit
        if method == "GET" and url == artifact["download_url"]:
            return 200, body, Message()
        return 403, b"", Message()

    monkeypatch.setattr(probe, "request_bytes", request_bytes)

    assert probe.verify_signed_artifact(artifact, "minio.example") == [
        "signed-artifact-85000000-0000-0000-0000-000000000131-method-tamper-rejected",
        "signed-artifact-85000000-0000-0000-0000-000000000131-path-tamper-rejected",
        "signed-artifact-85000000-0000-0000-0000-000000000131-version-tamper-rejected",
    ]


def test_run_probe_targets_persisted_fixture_job_ids(monkeypatch, tmp_path):
    own_project = "84000000-0000-0000-0000-000000000002"
    same_org_project = "85000000-0000-0000-0000-000000000002"
    other_org_project = "85000000-0000-0000-0000-000000000003"
    own_job = "3e4be0cc-fcc0-42fa-a502-6080df76c634"
    same_org_job = "85000000-0000-0000-0000-000000000101"
    other_org_job = "85000000-0000-0000-0000-000000000102"
    credential = tmp_path / "credential"
    credential.write_text("vla_synthetic", encoding="utf-8")
    environment = {
        "VELA_PROBE_BASE_URL": "http://control.example",
        "VELA_PROBE_OWN_PROJECT_ID": own_project,
        "VELA_PROBE_SAME_ORG_PROJECT_ID": same_org_project,
        "VELA_PROBE_OTHER_ORG_PROJECT_ID": other_org_project,
        "VELA_PROBE_JOB_ID": own_job,
        "VELA_PROBE_SAME_ORG_JOB_ID": same_org_job,
        "VELA_PROBE_OTHER_ORG_JOB_ID": other_org_job,
        "VELA_PROBE_SIGNED_HOST": "minio.example",
        "VELA_PROBE_CREDENTIAL_FILE": str(credential),
        "VELA_PROBE_SOURCE_SHA256": probe.file_sha256(PROBE_PATH),
    }
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    hidden_urls = []
    request_ids = iter([f"00000000-0000-4000-8000-{index:012d}" for index in range(1, 7)])

    def expect_hidden(url, bearer):
        assert bearer == "vla_synthetic"
        hidden_urls.append(url)
        return next(request_ids)

    def request_json(url, expected_status, bearer):
        assert expected_status == 200
        assert bearer == "vla_synthetic"
        if url.endswith("/artifacts"):
            return {
                "job_id": own_job,
                "artifacts": [
                    {"artifact_id": "video", "kind": "VIDEO"},
                    {"artifact_id": "thumbnail", "kind": "THUMBNAIL"},
                ],
            }, next(request_ids)
        return {"job_id": own_job, "project_id": own_project}, next(request_ids)

    def verify_signed_artifact(artifact, signed_host):
        assert signed_host == "minio.example"
        return [
            f"{artifact['artifact_id']}-{boundary}" for boundary in ("method", "path", "version")
        ]

    monkeypatch.setattr(probe, "expect_hidden", expect_hidden)
    monkeypatch.setattr(probe, "request_json", request_json)
    monkeypatch.setattr(probe, "verify_signed_artifact", verify_signed_artifact)

    receipt = probe.run_probe()

    assert hidden_urls == [
        f"http://control.example/v1/projects/{same_org_project}/jobs/{same_org_job}",
        f"http://control.example/v1/projects/{same_org_project}/jobs/{same_org_job}/artifacts",
        f"http://control.example/v1/projects/{other_org_project}/jobs/{other_org_job}",
        f"http://control.example/v1/projects/{other_org_project}/jobs/{other_org_job}/artifacts",
    ]
    assert receipt["foreign_job_ids"] == {
        "same_organization_foreign_project": same_org_job,
        "foreign_organization": other_org_job,
    }
    assert receipt["schema"] == "vela-lab-organization-isolation-http-probe-v3"
    assert receipt["api_request_correlations"] == [
        {
            "operation": "authorized-job-read",
            "request_id": "00000000-0000-4000-8000-000000000001",
        },
        {
            "operation": "same-organization-foreign-project-fixture-job-hidden",
            "request_id": "00000000-0000-4000-8000-000000000002",
        },
        {
            "operation": "same-organization-foreign-project-fixture-artifact-set-hidden",
            "request_id": "00000000-0000-4000-8000-000000000003",
        },
        {
            "operation": "foreign-organization-fixture-job-hidden",
            "request_id": "00000000-0000-4000-8000-000000000004",
        },
        {
            "operation": "foreign-organization-fixture-artifact-set-hidden",
            "request_id": "00000000-0000-4000-8000-000000000005",
        },
        {
            "operation": "authorized-artifact-read",
            "request_id": "00000000-0000-4000-8000-000000000006",
        },
    ]
    assert receipt["role_evidence_request_ids"] == {
        "job_read": "00000000-0000-4000-8000-000000000001",
        "artifact_read": "00000000-0000-4000-8000-000000000006",
    }
    assert receipt["negative_probe_count"] == len(receipt["negative_probes"]) == 10


def test_response_request_id_requires_one_server_uuid():
    headers = Message()
    headers.add_header("X-Request-ID", "00000000-0000-4000-8000-000000000001")
    assert probe.response_request_id(headers) == "00000000-0000-4000-8000-000000000001"

    with pytest.raises(probe.ProbeError, match="correlation was invalid"):
        probe.response_request_id(Message())
    duplicate = Message()
    duplicate.add_header("X-Request-ID", "00000000-0000-4000-8000-000000000001")
    duplicate.add_header("X-Request-ID", "00000000-0000-4000-8000-000000000002")
    with pytest.raises(probe.ProbeError, match="correlation was invalid"):
        probe.response_request_id(duplicate)


def test_run_probe_rejects_duplicate_job_identities(monkeypatch):
    job_id = "85000000-0000-0000-0000-000000000101"
    required = {
        "VELA_PROBE_BASE_URL": "http://control.example",
        "VELA_PROBE_OWN_PROJECT_ID": "84000000-0000-0000-0000-000000000002",
        "VELA_PROBE_SAME_ORG_PROJECT_ID": "85000000-0000-0000-0000-000000000002",
        "VELA_PROBE_OTHER_ORG_PROJECT_ID": "85000000-0000-0000-0000-000000000003",
        "VELA_PROBE_JOB_ID": job_id,
        "VELA_PROBE_SAME_ORG_JOB_ID": job_id,
        "VELA_PROBE_OTHER_ORG_JOB_ID": "85000000-0000-0000-0000-000000000102",
        "VELA_PROBE_SIGNED_HOST": "minio.example",
        "VELA_PROBE_CREDENTIAL_FILE": "/not-read",
        "VELA_PROBE_SOURCE_SHA256": "0" * 64,
    }
    for name, value in required.items():
        monkeypatch.setenv(name, value)

    with pytest.raises(probe.ProbeError, match="not distinct"):
        probe.run_probe()
