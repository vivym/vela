#!/usr/bin/env python3
"""Generate promtool fixtures for infrastructure failure semantics.

Requires PyYAML. Run with a PrometheusRule file and an output directory, plus
an optional hardware-federation manifest, then
run `promtool test rules <output>/tests.json`. Fixtures intentionally cover
failed scrapes, disappeared targets, healthy endpoints and partial redundancy.
"""
import json
import pathlib
import sys

import yaml

source = yaml.safe_load(pathlib.Path(sys.argv[1]).read_text())["spec"]
if len(sys.argv) > 3:
    for resource in yaml.safe_load_all(pathlib.Path(sys.argv[3]).read_text()):
        if resource and resource.get("kind") == "PrometheusRule":
            source["groups"].extend(resource["spec"]["groups"])
out = pathlib.Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)
(out / "rules.json").write_text(json.dumps(source))
rules = {r["alert"]: r for g in source["groups"] for r in g["rules"] if "alert" in r}
tests = []


def case(name, alert, series, expected):
    # Reuse display text, but derive all series/expected labels independently
    # of the rule expression. A label-dropping count() must fail these tests.
    r = rules[alert]
    tests.append({
        "name": name, "interval": "30s",
        "input_series": [{"series": metric, "values": values} for metric, values in series],
        "alert_rule_test": [{"eval_time": "20m", "alertname": alert,
                             "exp_alerts": [{"exp_labels": {**labels, **r.get("labels", {})},
                                             "exp_annotations": r.get("annotations", {})}
                                            for labels in expected]}],
    })


for alert, job in [
    ("VelaNodeExporterTargetMissing", "node-exporter"),
    ("VelaGPUExporterTargetMissing", "nvidia-smi"),
    ("VelaLonghornMetricsMissing", "longhorn-backend"),
    ("VelaCNPGMetricsMissing", "monitoring/cnpg-postgres"),
    ("VelaNATSMetricsMissing", "nats-prometheus-exporter"),
    ("VelaMinIOMetricsMissing", "minio"),
    ("VelaMinIOQuorumEndpointUnavailable", "minio-quorum"),
    ("VelaAPISIXMetricsMissing", "apisix-metrics"),
    ("VelaLogStoreUnavailable", "loki"),
    ("VelaTraceStoreUnavailable", "tempo"),
    ("VelaNodeLocalMetricsUnavailable", "node-local-metrics"),
]:
    metric = f'up{{job="{job}",instance="failed:1234"}}'
    case(job + " healthy", alert, [(metric, "1+0x40")], [])
    case(job + " failed scrape retains instance", alert, [(metric, "0+0x40")],
         [{"job": job, "instance": "failed:1234"}])
    case(job + " disappears", alert, [(metric, "1 stale")], [{"job": job}])
    case(job + " partial failure", alert,
         [(metric, "0+0x40"), (f'up{{job="{job}",instance="healthy:1234"}}', "1+0x40")],
         [{"job": job, "instance": "failed:1234"}])

if "VelaHardwareFederationUnavailable" in rules:
    alert = "VelaHardwareFederationUnavailable"
    labels = {"job": "legacy-hardware-federation", "instance": "legacy:9090"}
    metric = 'up{job="legacy-hardware-federation",instance="legacy:9090"}'
    case("federation up", alert, [(metric, "1+0x40")], [])
    case("federation down", alert, [(metric, "0+0x40")], [labels])
    case("federation disappears", alert, [], [{"job": "legacy-hardware-federation"}])
    for job in ["ipmi", "ipmi-supermicro"]:
        labels = {"job": job, "instance": "bmc-a", "collector": "bmc"}
        metric = f'ipmi_up{{job="{job}",instance="bmc-a",collector="bmc"}}'
        http = f'up{{job="{job}",instance="bmc-a"}}'
        case(job + " HTTP succeeds but BMC fails", "VelaIPMICollectionFailed",
             [(metric, "0+0x40"), (http, "1+0x40")], [labels])
        case(job + " BMC healthy", "VelaIPMICollectionFailed",
             [(metric, "1+0x40"), (http, "1+0x40")], [])
        case(job + " HTTP fails", "VelaIPMICollectionFailed", [(http, "0+0x40")],
             [{"job": job, "instance": "bmc-a"}])
        case(job + " hardware metrics present", "VelaIPMITelemetryMissing", [(metric, "1+0x40")], [])
    case("HTTP up but no BMC metrics", "VelaIPMITelemetryMissing",
         [('up{job="legacy-hardware-federation"}', "1+0x40")], [{}])

for alert, job, extra in [("VelaOTLPCollectorUnavailable", "otel-collector", ',endpoint="metrics"'),
                           ("VelaAPISIXMetricsRedundancyReduced", "apisix-metrics", "")]:
    a = f'up{{job="{job}",instance="a"{extra}}}'
    b = f'up{{job="{job}",instance="b"{extra}}}'
    case(job + " two healthy", alert, [(a, "1+0x40"), (b, "1+0x40")], [])
    case(job + " one down", alert, [(a, "1+0x40"), (b, "0+0x40")], [{}])
    case(job + " one disappears", alert, [(a, "1+0x40"), (b, "1 stale")], [{}])
    case(job + " all absent", alert, [], [{}])
    case(job + " all down", alert, [(a, "0+0x40"), (b, "0+0x40")], [{}])

nodes = [('kube_node_info{node="a"}', "1+0x40"), ('kube_node_info{node="b"}', "1+0x40")]
case("discovery loses one node", "VelaNodeExporterCoverageIncomplete",
     nodes + [('up{job="node-exporter",instance="a"}', "1+0x40")], [{}])
case("discovery complete even with failed scrape", "VelaNodeExporterCoverageIncomplete",
     nodes + [('up{job="node-exporter",instance="a"}', "1+0x40"),
              ('up{job="node-exporter",instance="b"}', "0+0x40")], [])
for state in ["healthy", "degraded", "faulted"]:
    labels = {"state": state, "volume": "test-volume", "pvc": "loki-data", "pvc_namespace": "monitoring"}
    metric = 'longhorn_volume_robustness{' + ','.join(f'{k}="{v}"' for k, v in labels.items()) + '}'
    case("Longhorn " + state, "VelaLonghornVolumeDegraded", [(metric, "1+0x40")],
         [] if state == "healthy" else [labels])

for free in [5, 50]:
    labels = 'instance="node-a",mountpoint="/",fstype="ext4"'
    case(f"filesystem {free} percent free", "VelaNodeFilesystemLow",
         [(f'node_filesystem_avail_bytes{{{labels}}}', f"{free}+0x40"),
          (f'node_filesystem_size_bytes{{{labels}}}', "100+0x40")],
         [{"instance": "node-a", "mountpoint": "/"}] if free < 10 else [])

for free in [5, 15, 19, 20, 50]:
    labels = 'instance="node-a",mountpoint="/",fstype="ext4"'
    case(f"root filesystem {free} percent free", "VelaNodeRootFilesystemHeadroomLow",
         [(f'node_filesystem_avail_bytes{{{labels}}}', f"{free}+0x40"),
          (f'node_filesystem_size_bytes{{{labels}}}', "100+0x40")],
         [{"instance": "node-a", "mountpoint": "/"}] if free < 20 else [])
case("data filesystem does not trigger root headroom warning", "VelaNodeRootFilesystemHeadroomLow",
     [('node_filesystem_avail_bytes{instance="node-a",mountpoint="/data",fstype="ext4"}', '5+0x40'),
      ('node_filesystem_size_bytes{instance="node-a",mountpoint="/data",fstype="ext4"}', '100+0x40')], [])
for free in [5, 10, 14, 15, 50]:
    labels = 'instance="node-a",mountpoint="/srv/models",fstype="zfs"'
    case(f"ZFS filesystem {free} percent free", "VelaZFSFilesystemLow",
         [(f'node_filesystem_avail_bytes{{{labels}}}', f"{free}+0x40"),
          (f'node_filesystem_size_bytes{{{labels}}}', "100+0x40")],
         [{"instance": "node-a", "mountpoint": "/srv/models"}] if free < 15 else [])
for free in [5, 9, 10, 15, 50]:
    labels = 'instance="node-a",mountpoint="/srv/models",fstype="zfs"'
    case(f"ZFS filesystem critical {free} percent free", "VelaZFSFilesystemCritical",
         [(f'node_filesystem_avail_bytes{{{labels}}}', f"{free}+0x40"),
          (f'node_filesystem_size_bytes{{{labels}}}', "100+0x40")],
         [{"instance": "node-a", "mountpoint": "/srv/models"}] if free < 10 else [])
case("ext4 filesystem does not trigger ZFS alerts", "VelaZFSFilesystemLow",
     [('node_filesystem_avail_bytes{instance="node-a",mountpoint="/",fstype="ext4"}', '5+0x40'),
      ('node_filesystem_size_bytes{instance="node-a",mountpoint="/",fstype="ext4"}', '100+0x40')], [])
case("ext4 filesystem does not trigger ZFS critical alert", "VelaZFSFilesystemCritical",
     [('node_filesystem_avail_bytes{instance="node-a",mountpoint="/",fstype="ext4"}', '5+0x40'),
      ('node_filesystem_size_bytes{instance="node-a",mountpoint="/",fstype="ext4"}', '100+0x40')], [])
disk_pressure = 'kube_node_status_condition{node="node-a",condition="DiskPressure",status="true"}'
for name, values, expected in [("active", "1+0x40", [{"node": "node-a"}]),
                               ("healthy", "0+0x40", []),
                               ("recovered", "1+0x10 0+0x30", []),
                               ("stale", "1+0x10 stale", [])]:
    case("node disk pressure " + name, "VelaNodeDiskPressure", [(disk_pressure, values)], expected)
case("duplicate kube-state-metrics keeps one node alert", "VelaNodeDiskPressure",
     [(disk_pressure, "1+0x40"),
      ('kube_node_status_condition{node="node-a",condition="DiskPressure",status="true",instance="replica-b"}', "1+0x40")],
     [{"node": "node-a"}])
case("non-disk conditions do not trigger disk pressure alert", "VelaNodeDiskPressure",
     [('kube_node_status_condition{node="node-a",condition="MemoryPressure",status="true"}', "1+0x40"),
      ('kube_node_status_condition{node="node-b",condition="DiskPressure",status="false"}', "1+0x40")], [])

for value in [1, 2]:
    labels = {"namespace": "monitoring", "persistentvolumeclaim": "loki-data", "volume": "pvc-loki"}
    metric = 'kube_customresource_longhorn_volume_replicas{' + ','.join(f'{k}="{v}"' for k, v in labels.items()) + '}'
    case(f"Loki healthy with {value} configured copies", "VelaLonghornReplicaPolicyViolated",
         [(metric, f"{value}+0x40"), ('longhorn_volume_robustness{state="healthy",volume="pvc-loki"}', "1+0x40")],
         [labels] if value < 2 else [])
for namespace, pvc, exempt in [("vela-system", "vela-control-abcd-artifact-validation", True),
                                ("customer", "vela-control-abcd-artifact-validation", False),
                                ("vela-system", "real-database", False)]:
    labels = {"namespace": namespace, "persistentvolumeclaim": pvc, "volume": "pvc-test"}
    metric = 'kube_customresource_longhorn_volume_replicas{' + ','.join(f'{k}="{v}"' for k, v in labels.items()) + '}'
    info = f'kube_persistentvolumeclaim_info{{namespace="{namespace}",persistentvolumeclaim="{pvc}",storageclass="longhorn-wffc"}}'
    case(f"Single copy exemption {namespace}/{pvc}", "VelaLonghornReplicaPolicyViolated",
         [(metric, "1+0x40"), (info, "1+0x40")], [] if exempt else [labels])
case("Replica policy metrics absent", "VelaLonghornReplicaPolicyMetricsMissing", [], [{}])
case("Replica policy misses one volume", "VelaLonghornReplicaPolicyMetricsMissing",
     [('kube_customresource_longhorn_volume_replicas{volume="a"}', "2+0x40"),
      ('longhorn_volume_capacity_bytes{volume="a"}', "100+0x40"),
      ('longhorn_volume_capacity_bytes{volume="b"}', "100+0x40")], [{}])

for alert, job, expected_count, endpoint in [
    ("VelaSchedulerMetricsIncomplete", "kube-scheduler", 3, ""),
    ("VelaControllerManagerMetricsIncomplete", "kube-controller-manager", 3, ""),
    ("VelaEtcdMetricsIncomplete", "etcd", 3, ""),
    ("VelaOTLPInternalMetricsIncomplete", "otel-collector", 2, ',endpoint="internal"'),
]:
    series = [(f'up{{job="{job}",instance="{i}"{endpoint}}}', "1+0x40") for i in range(expected_count)]
    case(job + " complete metrics", alert, series, [])
    case(job + " missing member", alert, series[:-1], [{}])
    case(job + " all metrics absent", alert, [], [{}])
case("Kube-proxy HTTP fails", "VelaKubeProxyMetricsMissing",
     [('up{job="kube-proxy",node="node-a"}', "0+0x40"), ('kube_node_info{node="node-a"}', "1+0x40")],
     [{"job": "kube-proxy", "node": "node-a"}])
case("Kube-proxy relay disappears", "VelaKubeProxyMetricsMissing",
     [('kube_node_info{node="node-a"}', "1+0x40")], [{"node": "node-a"}])
case("Kube-proxy node covered", "VelaKubeProxyMetricsMissing",
     [('up{job="kube-proxy",node="node-a"}', "1+0x40"), ('kube_node_info{node="node-a"}', "1+0x40")], [])
for value in [0, 1]:
    case(f"Etcd leader available {value}", "VelaEtcdLeaderMissing",
         [('etcd_server_has_leader{job="etcd",instance="node-a"}', f"{value}+0x40")],
         [{"job": "etcd", "instance": "node-a"}] if value == 0 else [])
for description, fast_bucket, expected in [("fast fsync", "100", []),
                                           ("slow fsync", "90", [{"instance": "node-a"}])]:
    case(description, "VelaEtcdDiskLatencyHigh",
         [('etcd_disk_wal_fsync_duration_seconds_bucket{job="etcd",instance="node-a",le="0.25"}', f"0+{fast_bucket}x40"),
          ('etcd_disk_wal_fsync_duration_seconds_bucket{job="etcd",instance="node-a",le="0.5"}', "0+100x40"),
          ('etcd_disk_wal_fsync_duration_seconds_bucket{job="etcd",instance="node-a",le="+Inf"}', "0+100x40")], expected)
for alert, counter, job in [("VelaOTLPSpanExportFailing", "otelcol_exporter_send_failed_spans_total", "otel-collector"),
                            ("VelaOTLPSpansRefused", "otelcol_receiver_refused_spans_total", "otel-collector"),
                            ("VelaLogEntriesDropped", "loki_write_dropped_entries_total", "alloy"),
                            ("VelaLogDeliveryRetrying", "loki_write_batch_retries_total", "alloy")]:
    for label, values, expected in [("healthy", "0+0x40", []), ("failing", "0+1x40", [{"job": job, "instance": "collector-a"}]),
                                     ("recovered", "0+1x10 10+0x30", [])]:
        case(counter + " " + label, alert,
             [(counter + '{job="' + job + '",instance="collector-a"}', values)], expected)
for state, series, expected in [
    ("missing", [], [{"node": "node-a"}]),
    ("failed", [('up{job="alloy",node="node-a"}', "0+0x40")], [{"node": "node-a"}]),
    ("healthy", [('up{job="alloy",node="node-a"}', "1+0x40")], []),
]:
    case("Alloy metrics " + state, "VelaLogCollectorMetricsMissing",
         [('kube_node_info{node="node-a"}', "1+0x40")] + series, expected)

for online in [4, 3, 2, 1]:
    # Four scrape endpoints report the same erasure set; never sum drives.
    series = [(f'minio_cluster_erasure_set_{metric}{{job="minio-quorum",namespace="object-store",service="minio",pool_id="0",set_id="0",instance="{instance}"}}', f'{value}+0x40')
              for instance in ['a', 'b']
              for metric, value in [('online_drives_count', online), ('write_quorum', 3), ('read_quorum', 2)]]
    for alert, fires in [('VelaMinIOWriteQuorumLost', online < 3),
                         ('VelaMinIOReadQuorumLost', online < 2),
                         ('VelaMinIOWriteQuorumAtLimit', online == 3)]:
        case(f'{alert} with {online} drives and duplicate scrapes', alert, series,
             [{'namespace': 'object-store', 'service': 'minio', 'pool_id': '0', 'set_id': '0'}] if fires else [])
for missing in [None, 'online_drives_count', 'write_quorum', 'read_quorum']:
    series = [('up{job="minio-quorum",instance="a"}', '1+0x40')]
    for instance in ['a', 'b']:
        series += [(f'minio_cluster_erasure_set_{metric}{{job="minio-quorum",pool_id="0",set_id="0",instance="{instance}"}}', f'{value}+0x40')
                   for metric, value in [('online_drives_count', 4), ('write_quorum', 3), ('read_quorum', 2)]
                   if instance == 'b' or metric != missing]
    case(f'MinIO partial target telemetry missing {missing}', 'VelaMinIOQuorumMetricsMissing', series,
         [{'job': 'minio-quorum', 'instance': 'a'}] if missing else [])
case('MinIO all quorum series disappear', 'VelaMinIOQuorumMetricsMissing', [], [{'job': 'minio-quorum'}])

# The full v2 inventory can time out when two peers are blackholed, while the
# dedicated v3 storage-layer collector and S3 continue to work. Keep its health
# independent, and never merge separate clusters which both have pool/set 0.
series = [('up{job="minio",instance="a"}', '0+0x40'),
          ('up{job="minio-quorum",instance="a"}', '1+0x40')]
for service, online, write, read in [('minio', 2, 3, 2), ('minio-ha', 6, 4, 3)]:
    series += [(f'minio_cluster_erasure_set_{metric}{{job="minio-quorum",namespace="object-store",service="{service}",pool_id="0",set_id="0",instance="{service}"}}', f'{value}+0x40')
               for metric, value in [('online_drives_count', online), ('write_quorum', write), ('read_quorum', read)]]
case('Separate MinIO clusters retain their own quorum', 'VelaMinIOWriteQuorumLost', series,
     [{'namespace': 'object-store', 'service': 'minio', 'pool_id': '0', 'set_id': '0'}])
case('v2 inventory failure does not lose v3 endpoint', 'VelaMinIOQuorumEndpointUnavailable', series, [])

(out / "tests.json").write_text(json.dumps({"rule_files": ["rules.json"],
                                            "evaluation_interval": "30s", "tests": tests}))
print(f"Generated {len(tests)} failure/healthy fixtures in {out}")
