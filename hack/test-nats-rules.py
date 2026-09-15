#!/usr/bin/env python3
"""Generate promtool cases for independent NATS members and duplicate exporters."""
import json
from pathlib import Path
import sys

import yaml

rules = yaml.safe_load(Path(sys.argv[1]).read_text())["spec"]
out = Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)
(out / "rules.json").write_text(json.dumps(rules))
alerts = {r["alert"]: r for g in rules["groups"] for r in g["rules"] if "alert" in r}
tests = []
job = "nats-prometheus-exporter"


def series(metric, members=(0, 1, 2), copies=2, value="1+0x50", **labels):
    return [(metric + "{" + ",".join(f'{k}="{v}"' for k, v in
             dict(job=job, nats_member=f"nats-{m}", instance=f"cpu-{c}:{7777+m}", **labels).items()) + "}", value)
            for m in members for c in range(copies)]


def case(name, alert, inputs, expected):
    rule = alerts[alert]
    tests.append({"name": name, "interval": "30s",
                  "input_series": [{"series": metric, "values": values} for metric, values in inputs],
                  "alert_rule_test": [{"eval_time": "20m", "alertname": alert,
                                       "exp_alerts": [{"exp_labels": {**x, **rule["labels"]},
                                                       "exp_annotations": rule["annotations"]} for x in expected]}]})


metric = "jetstream_server_max_storage"
for members in [(0, 1, 2), (0, 1), ()]:
    case(f"member coverage {members}", "VelaNATSMemberCoverageIncomplete",
         series(metric, members), [] if len(members) == 3 else [{}])
case("two copies of same member are not quorum", "VelaNATSMemberCoverageIncomplete", series(metric, (0,), copies=6), [{}])
case("HTTP up but member JSZ missing", "VelaNATSMemberCoverageIncomplete", series("up"), [{}])
case("member disappears", "VelaNATSMemberCoverageIncomplete",
     series(metric, (0, 1)) + series(metric, (2,), value="1 stale"), [{}])
case("member recovers", "VelaNATSMemberCoverageIncomplete",
     series(metric, (0, 1)) + series(metric, (2,), value="_ _ 1+0x48"), [])
case("redundant exporters", "VelaNATSExporterRedundancyReduced", series("up"), [])
case("one exporter host lost", "VelaNATSExporterRedundancyReduced", series("up", copies=1),
     [{"nats_member": f"nats-{i}"} for i in range(3)])
for size in [39378487296, 68719476736, 77309411328]:
    case(f"server quota {size}", "VelaNATSFileQuotaBelowContract", series(metric, value=f"{size}+0x50"),
         [{"nats_member": f"nats-{i}"} for i in range(3)] if size < 68719476736 else [])
case("one insufficient member", "VelaNATSFileQuotaBelowContract",
     series(metric, (0, 1), value="77309411328+0x50") + series(metric, (2,), value="39378487296+0x50"),
     [{"nats_member": "nats-2"}])
stream = dict(stream_name="VELA_EVENTS")
missing_stream = [dict(job=job, **stream)]
case("no business stream", "VelaNATSEventStreamMissing", series("up"), missing_stream)
case("other stream is not business stream", "VelaNATSEventStreamMissing",
     series("jetstream_stream_total_bytes", stream_name="OTHER"), missing_stream)
case("empty business stream exists", "VelaNATSEventStreamMissing",
     series("jetstream_stream_total_bytes", value="0+0x50", **stream), [])
case("business stream lost", "VelaNATSEventStreamMissing",
     series("jetstream_stream_total_bytes", value="1 stale", **stream), missing_stream)
for members in [(0, 1, 2), (0, 1), ()]:
    case(f"stream replica coverage {members}", "VelaNATSEventStreamReplicaCoverageIncomplete",
         series("jetstream_stream_total_bytes", members, **stream), [{}] if 0 < len(members) < 3 else [])
consumer = dict(**stream, consumer_name="VELA_SCHEDULER")
case("consumer absent", "VelaNATSSchedulerConsumerMissing",
     series("jetstream_stream_total_bytes", **stream), [dict(job=job, **consumer)])
case("consumer with no pending messages exists", "VelaNATSSchedulerConsumerMissing",
     series("jetstream_consumer_num_pending", value="0+0x50", **consumer), [])
case("one agreed leader", "VelaNATSMetadataLeaderDisagreement",
     series("jetstream_server_total_streams", meta_leader="nats-0"), [])
case("two reported leaders", "VelaNATSMetadataLeaderDisagreement",
     series("jetstream_server_total_streams", (0, 1), meta_leader="nats-0") +
     series("jetstream_server_total_streams", (2,), meta_leader="nats-2"), [{}])
for pending in [0, 1000, 1001]:
    case(f"scheduler backlog {pending}", "VelaNATSSchedulerBacklog",
         series("jetstream_consumer_num_pending", value=f"{pending}+0x50", **consumer),
         [consumer] if pending > 1000 else [])
(out / "tests.json").write_text(json.dumps({"rule_files": [str(out / "rules.json")], "evaluation_interval": "30s", "tests": tests}))
print(f"Generated {len(tests)} NATS alert scenarios")
