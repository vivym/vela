#!/usr/bin/env python3
"""Inventory and migrate the isolated MarsLab MinIO cluster using its native mc.

Run on llmpool01 with root kubectl access. Sensitive metadata exports stay in a
0700 remote directory, never in stdout or the repository. Source reads use the
original headless service, so changing the client Service cannot redirect them.
"""

import argparse
import datetime
import hashlib
import io
import json
import os
import pathlib
import shlex
import subprocess
import zipfile
from urllib.parse import urlsplit, unquote

ROOT = pathlib.Path("/opt/vela-cluster/minio-migration")


def run(args, data=None, timeout=120):
    p = subprocess.run(args, input=data, capture_output=True, timeout=timeout)
    if p.returncode:
        # mc diagnostics can contain credentials; report only the failed tool.
        raise RuntimeError("command failed: " + " ".join(args[:4]) + "; rc=" + str(p.returncode))
    return p.stdout


def kube(*args):
    return json.loads(run(["kubectl", *args, "-o", "json"]))


def pod_script(script, timeout=120):
    prefix = '''set -eu
umask 077
work=$(mktemp -d /tmp/vela-minio-migration.XXXXXX)
trap 'rm -rf "$work"' EXIT
cd "$work"
mc --config-dir "$work/config" alias set src http://minio-headless.object-store.svc.cluster.local:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
mc --config-dir "$work/config" alias set dst http://minio-ha.object-store.svc.cluster.local:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
m() { mc --config-dir "$work/config" "$@"; }
'''
    return run(["kubectl", "-n", "object-store", "exec", "-i", "minio-0", "--",
                "sh", "-s"], data=(prefix + script).encode(), timeout=timeout)


def mc(*args):
    raw = pod_script("m " + " ".join(map(shlex.quote, args)))
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def copyable_inventory(alias="src"):
    objects = {}
    for b in mc("ls", alias, "--json"):
        bucket = b["key"].rstrip("/")
        entries = mc("ls", "--versions", "--recursive", "--json", alias + "/" + bucket)
        versioning = mc("version", "info", alias + "/" + bucket, "--json")[0].get("versioning", {})
        if versioning.get("status") == "Enabled" and entries:
            raise RuntimeError("versioned source objects require native version-preserving replication")
        if any(e.get("status") != "success" or e.get("type") != "file" or
               e.get("versionId") not in (None, "", "null") or
               e.get("isDeleteMarker") or e.get("isDelete") for e in entries):
            raise RuntimeError("source has versions, delete markers, or incomplete inventory")
        objects[bucket] = entries
    return objects


def archive_entries(content):
    with zipfile.ZipFile(io.BytesIO(content)) as zipped:
        names = zipped.namelist()
        if len(names) != len(set(names)) or sum(zipped.getinfo(n).file_size for n in names) > 16 * 1024**2:
            raise RuntimeError("invalid metadata archive")
        return {n: zipped.read(n) for n in names}


def policy_canonical(value):
    # IAM Statement, Action, Resource and principal lists are unordered sets.
    if isinstance(value, dict):
        return {k: policy_canonical(v) for k, v in value.items()}
    if isinstance(value, list):
        return sorted((policy_canonical(v) for v in value), key=lambda v: json.dumps(v, sort_keys=True))
    return value


def metadata_equal(name, left, right):
    if left == right:
        return True
    if name.endswith(".json"):
        try:
            a, b = json.loads(left), json.loads(right)
        except (ValueError, UnicodeDecodeError):
            return False
        if name == "iam-assets/policies.json":
            a, b = policy_canonical(a), policy_canonical(b)
        return a == b
    return False


def export_metadata(alias):
    raw = pod_script('''m admin cluster bucket export ''' + shlex.quote(alias) + ''' >/dev/null
set -- ./*-bucket-metadata.zip
[ "$#" -eq 1 ] && [ -f "$1" ]
wc -c < "$1"
cat "$1"
m admin cluster iam export ''' + shlex.quote(alias) + ''' --output iam.zip >/dev/null
wc -c < iam.zip
cat iam.zip
''')
    stream, result = io.BytesIO(raw), {}
    for name in ("buckets.zip", "iam.zip"):
        length = int(stream.readline().strip())
        if not 0 < length < 16 * 1024**2:
            raise RuntimeError("metadata frame exceeds bounds")
        result[name] = stream.read(length)
        if len(result[name]) != length:
            raise RuntimeError("truncated metadata frame")
    if stream.read():
        raise RuntimeError("unexpected export trailer")
    return result


def object_identity(entry):
    return tuple(entry.get(k) for k in ("key", "size", "etag", "lastModified", "versionId"))


def snapshot_changes(before, after):
    left = {(b, e["key"]): object_identity(e) for b, es in before.items() for e in es}
    right = {(b, e["key"]): object_identity(e) for b, es in after.items() for e in es}
    if set(before) != set(after) or any(right.get(k) != v for k, v in left.items()):
        raise RuntimeError("source snapshot changed or objects were removed during verification")
    return len(right.keys() - left.keys())


def verify(directory):
    preflight()
    # Verify the recorded synchronized snapshot, explicitly reporting later WAL
    # additions. This is evidence for a final delta, never an automatic cutover.
    objects = json.loads((directory / "sync-source-objects.json").read_text())
    source = copyable_inventory()
    snapshot_changes(objects, source)
    destination = copyable_inventory("dst")
    if set(destination) != set(objects):
        raise RuntimeError("destination bucket set differs")
    want = {(b, e["key"]) for b, es in objects.items() for e in es}
    got = {(b, e["key"]) for b, es in destination.items() for e in es}
    if want != got:
        raise RuntimeError("destination object set differs from synchronized snapshot")
    exports = {alias: export_metadata(alias) for alias in ("src", "dst")}
    metadata_counts = {}
    for filename in ("buckets.zip", "iam.zip"):
        a, b = (archive_entries(exports[alias][filename]) for alias in ("src", "dst"))
        if set(a) != set(b) or any(not metadata_equal(n, a[n], b[n]) for n in a):
            raise RuntimeError("native bucket or IAM metadata differs")
        metadata_counts[filename] = len(a)
    stats, tags = {}, {}
    for alias in ("src", "dst"):
        stats[alias], tags[alias] = {}, {}
        for bucket in sorted(objects):
            # mc stat --recursive returns an error for a verified empty bucket.
            # Absence comes from the successful full version listing, never from
            # interpreting that CLI error as an empty/healthy result.
            if not objects[bucket]:
                continue
            for entry in mc("stat", "--recursive", "--json", alias + "/" + bucket):
                if entry.get("status") != "success" or entry.get("type") != "file":
                    raise RuntimeError("object stat failed")
                stats[alias][entry["name"]] = entry
            for entry in mc("tag", "list", "--recursive", "--json", alias + "/" + bucket):
                if entry.get("status") != "success":
                    raise RuntimeError("object tag inventory failed")
                name = unquote(urlsplit(entry["url"]).path).lstrip("/")
                tags[alias][name] = {k: v for k, v in entry.items() if k not in ("url", "status", "versionID")}
    for bucket, key in want:
        name = bucket + "/" + key
        if any(name not in stats[a] or name not in tags[a] for a in ("src", "dst")):
            raise RuntimeError("object metadata or tag inventory incomplete")
        if any(stats["src"][name].get(k) != stats["dst"][name].get(k) for k in ("size", "metadata")):
            raise RuntimeError("object metadata differs")
        if tags["src"][name] != tags["dst"][name]:
            raise RuntimeError("object tags differ")
    commands, ordered = [], sorted(want)
    for index, (bucket, key) in enumerate(ordered):
        for alias in ("src", "dst"):
            # Check GET success before hashing, so a partial/failed pipeline can
            # never pass as an empty object. Bound temporary disk use per object.
            size = stats[alias][bucket + "/" + key]["size"]
            if size > 512 * 1024**2:
                raise RuntimeError("object exceeds bounded verifier spool size")
            target = shlex.quote(alias + "/" + bucket + "/" + key)
            commands.append('m cat ' + target + ' > "$work/object"\n'
                            + 'printf "' + str(index) + ' ' + alias + ' "\n'
                            + 'sha256sum "$work/object"\nrm "$work/object"')
    # Feed the script over stdin; large object lists must not exceed exec argv.
    hashes = pod_script("\n".join(commands), timeout=600).decode().splitlines()
    if len(hashes) != len(ordered) * 2:
        raise RuntimeError("incomplete content hash inventory")
    receipts = []
    for index, (bucket, key) in enumerate(ordered):
        pairs = [line.split() for line in hashes[index * 2:index * 2 + 2]]
        if any(len(p) != 4 or p[:2] != [str(index), alias] or len(p[2]) != 64
               for p, alias in zip(pairs, ("src", "dst"))):
            raise RuntimeError("invalid hash receipt")
        if pairs[0][2] != pairs[1][2]:
            raise RuntimeError("object content differs")
        receipts.append({"bucket": bucket, "key": key, "sha256": pairs[0][2],
                         "size": stats["src"][bucket + "/" + key]["size"]})
    added = snapshot_changes(objects, copyable_inventory())
    result = {"time": datetime.datetime.now(datetime.timezone.utc).isoformat(), "mode": "verify",
              "result": "SYNCHRONIZED_SNAPSHOT_VERIFIED", "objects": len(receipts),
              "bytes": sum(r["size"] for r in receipts), "hash_algorithm": "SHA256",
              "object_metadata_equal": True, "object_tags_equal": True,
              "native_metadata_equal_entries": metadata_counts,
              "source_additions_since_sync": added, "cutover_performed": False,
              "manifest_sha256": hashlib.sha256(json.dumps(receipts, sort_keys=True).encode()).hexdigest(),
              "limits": ["lastModified and multipart ETag are not preserved by ordinary S3 copy",
                         "writer coordination and a final delta are required before cutover"]}
    write_private(directory / "verified-objects.json", receipts)
    write_private(directory / "verify-receipt.json", result)
    print(json.dumps(result, indent=2))


def migration_directory(value):
    directory = pathlib.Path(value).resolve()
    if directory.parent != ROOT or not (directory / "inventory.json").is_file():
        raise RuntimeError("use a previously recorded inventory directory")
    return directory


def sync(directory):
    preflight()
    original = json.loads((directory / "inventory.json").read_text())
    objects = copyable_inventory()
    if set(objects) != {b["name"] for b in original["buckets"]}:
        raise RuntimeError("source bucket inventory changed")
    destination = {b["key"].rstrip("/") for b in mc("ls", "dst", "--json")}
    if destination - set(objects):
        raise RuntimeError("destination contains unowned buckets")
    write_private(directory / "sync-source-objects.json", objects)
    # Native import preserves opaque bucket metadata and all IAM materials.
    # Export/import inside the same private Pod directory: never put secret
    # archive bytes in an exec command, a ConfigMap, or the Kubernetes audit log.
    output = pod_script('''m admin cluster bucket export src >/dev/null
set -- ./*-bucket-metadata.zip
[ "$#" -eq 1 ] && [ -f "$1" ]
m admin cluster bucket import dst "$1" >/dev/null
m admin cluster iam export src --output iam.zip >/dev/null
m admin cluster iam import dst iam.zip >/dev/null
'''+"\n".join("m mirror --overwrite --preserve " + shlex.quote("src/" + b) +
               " " + shlex.quote("dst/" + b) + " >/dev/null" for b in sorted(objects)), timeout=600)
    if output.strip():
        raise RuntimeError("unexpected synchronization output")
    after = copyable_inventory()
    write_private(directory / "sync-source-after.json", after)
    result = {"time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "mode": "sync", "native_metadata_and_iam_imported": True,
              "snapshot_objects": sum(len(es) for es in objects.values()),
              "snapshot_bytes": sum(e["size"] for es in objects.values() for e in es),
              "source_entries_after_sync": sum(len(es) for es in after.values()),
              "cutover_performed": False, "content_verification_pending": True}
    write_private(directory / "sync-receipt.json", result)
    print(json.dumps(result, indent=2))


def write_private(path, value):
    content = json.dumps(value, indent=2).encode()
    with open(path, "wb") as f:
        os.chmod(path, 0o600)
        f.write(content)


def preflight():
    if run(["hostname"]).decode().strip() != "llmpool01":
        raise RuntimeError("run on the accepted management host llmpool01")
    for service, selector in [("minio", {"app": "minio"}),
                              ("minio-headless", {"app": "minio"}),
                              ("minio-ha", {"app": "minio-ha"})]:
        if kube("-n", "object-store", "get", "svc", service)["spec"]["selector"] != selector:
            raise RuntimeError("source/destination service identity changed")
    for selector, count in [("app=minio", 4), ("app=minio-ha", 6)]:
        pods = kube("-n", "object-store", "get", "pods", "-l", selector)["items"]
        if len(pods) != count or any(not any(c["type"] == "Ready" and c["status"] == "True"
                                          for c in p["status"].get("conditions", [])) for p in pods):
            raise RuntimeError("source/destination members are not all Ready")
    ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(ROOT, 0o700)


def inventory():
    preflight()
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    directory = ROOT / stamp
    directory.mkdir(mode=0o700)
    archive = pod_script('''m admin cluster bucket export src >/dev/null
set -- ./*-bucket-metadata.zip
[ "$#" -eq 1 ] && [ -f "$1" ]
mv "$1" buckets.zip
m admin cluster iam export src --output iam.zip >/dev/null
wc -c < buckets.zip
cat buckets.zip
wc -c < iam.zip
cat iam.zip
''')
    exports = {}
    framed = io.BytesIO(archive)
    for name in ["buckets.zip", "iam.zip"]:
        size = int(framed.readline().strip())
        if not 0 < size < 16 * 1024 * 1024:
            raise RuntimeError("metadata export exceeds migration bounds")
        content = framed.read(size)
        if len(content) != size:
            raise RuntimeError("truncated metadata export")
        path = directory / name
        with open(path, "xb") as f:
            os.chmod(path, 0o600)
            f.write(content)
        summary = []
        with zipfile.ZipFile(io.BytesIO(content)) as zipped:
            for entry_name in zipped.namelist():
                row = {"entry": entry_name, "bytes": zipped.getinfo(entry_name).file_size}
                if entry_name.endswith(".json"):
                    try:
                        data = json.loads(zipped.read(entry_name))
                        row.update({"type": type(data).__name__, "entries": len(data)})
                    except (ValueError, TypeError):
                        row["json_summary"] = "not a collection"
                summary.append(row)
        exports[name] = summary
    if framed.read():
        raise RuntimeError("unexpected trailing export bytes")
    objects, buckets = {}, []
    for b in mc("ls", "src", "--json"):
        name = b["key"].rstrip("/")
        entries = mc("ls", "--versions", "--recursive", "--json", "src/" + name)
        if any(e.get("status") != "success" for e in entries):
            raise RuntimeError("source object inventory is incomplete")
        objects[name] = entries
        versioning = mc("version", "info", "src/" + name, "--json")
        buckets.append({"name": name, "entries": len(entries),
                        "bytes": sum(int(e.get("size", 0)) for e in entries),
                        "versioning": versioning[0].get("versioning"),
                        "entry_fields": sorted({k for e in entries for k in e}),
                        "non_null_version_ids": sum(e.get("versionId") not in (None, "", "null") for e in entries),
                        "delete_markers": sum(bool(e.get("isDeleteMarker") or e.get("isDelete")) for e in entries)})
    write_private(directory / "source-objects.json", objects)
    result = {"time": stamp, "mode": "inventory", "directory": str(directory),
              "exports": exports, "buckets": buckets,
              "destination_buckets": [b["key"].rstrip("/") for b in mc("ls", "dst", "--json")]}
    write_private(directory / "inventory.json", result)
    print(json.dumps(result, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["inventory", "sync", "verify"])
    parser.add_argument("--inventory-directory")
    args = parser.parse_args()
    if args.mode == "inventory":
        inventory()
    else:
        if not args.inventory_directory:
            parser.error(args.mode + " requires --inventory-directory")
        {"sync": sync, "verify": verify}[args.mode](migration_directory(args.inventory_directory))


if __name__ == "__main__":
    main()
