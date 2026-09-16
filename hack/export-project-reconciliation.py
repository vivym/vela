#!/usr/bin/env python3
"""Export a Project's immutable Charges from a management node, read-only.

The relay keeps its own order-to-job_id mapping and retail prices. This export
contains Vela costs, without prompts, credentials, or signed download URLs.
"""
import argparse
import csv
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import subprocess
import uuid


def utc(value):
    parsed = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('timestamps must include an explicit UTC offset')
    return parsed.astimezone(dt.timezone.utc)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project-id', required=True, type=uuid.UUID)
    parser.add_argument('--from', dest='start', required=True, type=utc)
    parser.add_argument('--to', dest='end', required=True, type=utc)
    parser.add_argument('--output-directory', required=True, type=Path)
    parser.add_argument('--kubectl', default='/var/lib/rancher/rke2/bin/kubectl')
    parser.add_argument('--kubeconfig', default='/etc/rancher/rke2/rke2.yaml')
    args = parser.parse_args()
    if args.start >= args.end:
        parser.error('--from must precede --to (end is exclusive)')
    os.umask(0o077)
    command = [args.kubectl, '--kubeconfig', args.kubeconfig, '-n', 'vela-system']

    def run(extra, stdin=None):
        return subprocess.check_output(command + extra, input=stdin, text=True, timeout=120)

    cluster = json.loads(run(['get', 'cluster', 'vela-postgres', '-o', 'json']))
    primary = cluster['status']['currentPrimary']
    # UUID and timezone-aware timestamps are parsed and normalized before SQL.
    project = str(args.project_id)
    start, end = args.start.isoformat(), args.end.isoformat()
    sql = f"""BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='60s';
SELECT json_build_object(
 'observed_at',transaction_timestamp(), 'project_id',p.id,
 'organization_id',p.organization_id, 'project_name',p.display_name,
 'currency',a.currency, 'contract_credit_limit_minor',a.contract_credit_limit_minor,
 'reserved_minor',a.reserved_minor, 'unsettled_posted_minor',a.unsettled_posted_minor,
 'available_credit_minor',a.contract_credit_limit_minor-a.reserved_minor-a.unsettled_posted_minor,
 'charges',coalesce((SELECT json_agg(row_to_json(c)) FROM (
   SELECT charge.id charge_id,charge.job_id,charge.artifact_set_id,charge.reason,
     charge.state,charge.currency,charge.amount_minor,charge.posted_at
   FROM charges charge WHERE charge.project_id=p.id AND charge.organization_id=p.organization_id
     AND charge.posted_at>='{start}'::timestamptz AND charge.posted_at<'{end}'::timestamptz
   ORDER BY charge.posted_at,charge.id) c),'[]'::json),
 'organization_finance_records',coalesce((SELECT json_agg(row_to_json(f)) FROM (
   SELECT id,kind,currency,settlement_minor,credit_adjustment_minor,contract_credit_limit_minor,
     external_reference,effective_at,posted_at
   FROM finance_reconciliation_records WHERE organization_id=p.organization_id
     AND posted_at>='{start}'::timestamptz AND posted_at<'{end}'::timestamptz
   ORDER BY posted_at,id) f),'[]'::json)
) FROM projects p JOIN organization_credit_accounts a ON a.organization_id=p.organization_id
WHERE p.id='{project}'::uuid;
COMMIT;
"""
    wire = run(['exec', '-i', primary, '-c', 'postgres', '--', 'psql', '-U', 'postgres',
                '-d', 'app', '-XqAt', '-v', 'ON_ERROR_STOP=1'], sql)
    if not wire.strip():
        raise ValueError('Project or credit account does not exist')
    report = json.loads(wire)
    charges = report.pop('charges')
    if len({c['charge_id'] for c in charges}) != len(charges):
        raise ValueError('duplicate Charge in reconciliation snapshot')
    if any(c['currency'] != report['currency'] or c['state'] != 'POSTED' for c in charges):
        raise ValueError('unexpected Charge currency or state')
    report.update(schema_version=1, interval_start=start, interval_end_exclusive=end,
                  charge_count=len(charges), total_amount_minor=sum(c['amount_minor'] for c in charges))
    target = args.output_directory
    target.mkdir(mode=0o700, parents=True, exist_ok=True)
    charge_path = target / 'charges.csv'
    with charge_path.open('w', newline='') as output:
        writer = csv.DictWriter(output, fieldnames=['charge_id', 'job_id', 'artifact_set_id',
                                'reason', 'state', 'currency', 'amount_minor', 'posted_at'])
        writer.writeheader()
        writer.writerows(charges)
    report['charges_csv_sha256'] = hashlib.sha256(charge_path.read_bytes()).hexdigest()
    (target / 'summary.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'project_id':project, 'charge_count':len(charges),
                      'total_amount_minor':report['total_amount_minor'],
                      'available_credit_minor':report['available_credit_minor'],
                      'output_directory':str(target)}, indent=2))


if __name__ == '__main__':
    main()
