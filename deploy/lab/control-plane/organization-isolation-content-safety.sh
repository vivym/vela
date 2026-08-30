#!/bin/sh

set -eu
umask 077

namespace=vela-lab
control_host=marslab-server
own_organization_id=84000000-0000-0000-0000-000000000001
own_project_id=84000000-0000-0000-0000-000000000002
own_principal_id=84000000-0000-0000-0000-000000000003
same_org_project_id=85000000-0000-0000-0000-000000000002
same_org_principal_id=85000000-0000-0000-0000-000000000005
other_organization_id=85000000-0000-0000-0000-000000000001
other_org_project_id=85000000-0000-0000-0000-000000000003
other_org_principal_id=85000000-0000-0000-0000-000000000004
same_org_job_id=85000000-0000-0000-0000-000000000101
other_org_job_id=85000000-0000-0000-0000-000000000102
same_org_attempt_id=85000000-0000-0000-0000-000000000111
other_org_attempt_id=85000000-0000-0000-0000-000000000112
same_org_artifact_set_id=85000000-0000-0000-0000-000000000121
other_org_artifact_set_id=85000000-0000-0000-0000-000000000122
same_org_video_id=85000000-0000-0000-0000-000000000131
same_org_thumbnail_id=85000000-0000-0000-0000-000000000132
other_org_video_id=85000000-0000-0000-0000-000000000133
other_org_thumbnail_id=85000000-0000-0000-0000-000000000134
same_org_grant_id=85000000-0000-0000-0000-000000000141
other_org_grant_id=85000000-0000-0000-0000-000000000142
probe_credential_id=85000000-0000-0000-0000-00000000000f
invalid_credential_id=85000000-0000-0000-0000-000000000010
probe_name=vela-lab-organization-isolation-probe
probe_configmap=vela-lab-organization-isolation-probe
expected_runner_image=10.1.200.17:5443/vela-lab/vela-h3-runner@sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}
probe_source=${1:-}
runner_image=${2:-}
output=${3:-}
apply=${4:-}
temporary=
fixture_owned=false
probe_uid=
configmap_uid=
committed=false

fail() {
	printf 'organization-isolation-content-safety: %s\n' "$*" >&2
	exit 1
}

query_database() {
	sql=$1
	# PostgreSQL credentials are expanded only inside the database container.
	# shellcheck disable=SC2016
	printf '%s\n' "$sql" | "$kubectl_bin" exec --stdin --namespace "$namespace" \
		statefulset/vela-lab-postgres -- sh -ec \
		'PGPASSWORD=$POSTGRES_PASSWORD exec psql --no-psqlrc --quiet --set=ON_ERROR_STOP=1 --tuples-only --no-align --field-separator="|" --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"'
}

expected_database_surfaces() {
	cat <<'EOF'
VELA_ARTIFACT_REPLICATION_DATABASE_URL|vela_artifact_replication
VELA_ARTIFACT_REQUEST_DATABASE_URL|vela_artifact_request
VELA_AUTH_DATABASE_URL|vela_auth
VELA_BACKUP_RETENTION_DATABASE_URL|vela_backup_retention
VELA_BILLING_DATABASE_URL|vela_billing
VELA_BREAK_GLASS_AUDIT_DATABASE_URL|vela_break_glass_audit_request
VELA_BREAK_GLASS_REQUEST_DATABASE_URL|vela_break_glass_request
VELA_CANCEL_DATABASE_URL|vela_cancel
VELA_COMPLIANCE_DATABASE_URL|vela_compliance
VELA_DEBUG_DUMP_AUDIT_REQUEST_DATABASE_URL|vela_debug_dump_audit_request
VELA_DEBUG_DUMP_REQUEST_DATABASE_URL|vela_debug_dump_request
VELA_FINANCE_RECONCILIATION_DATABASE_URL|vela_finance_reconciliation
VELA_FLEET_DATABASE_URL|vela_fleet
VELA_HUMAN_AUTH_DATABASE_URL|vela_human_auth
VELA_HUMAN_MEMBERSHIP_AUTH_DATABASE_URL|vela_human_membership_auth
VELA_HUMAN_MEMBERSHIP_REQUEST_DATABASE_URL|vela_human_membership_request
VELA_IDENTITY_REQUEST_DATABASE_URL|vela_identity_request
VELA_INTERNAL_DATABASE_URL|vela_internal
VELA_NON_CONTENT_EXPIRY_DATABASE_URL|vela_non_content_expiry
VELA_ORGANIZATION_AUDIT_REQUEST_DATABASE_URL|vela_organization_audit_request
VELA_ORGANIZATION_BILLING_REQUEST_DATABASE_URL|vela_organization_billing_request
VELA_PLATFORM_OPERATOR_AUTH_DATABASE_URL|vela_platform_operator_auth
VELA_REMEDIATION_DATABASE_URL|vela_remediation
VELA_REQUEST_DATABASE_URL|vela_request
VELA_RETENTION_DATABASE_URL|vela_retention
VELA_RETENTION_REQUEST_DATABASE_URL|vela_retention_request
VELA_SCHEDULER_DATABASE_URL|vela_scheduler
VELA_SCHEDULER_INBOX_DATABASE_URL|vela_scheduler_inbox
VELA_WEBHOOK_DATABASE_URL|vela_webhook
VELA_WEBHOOK_REQUEST_DATABASE_URL|vela_webhook_request
EOF
}

expect_sqlstate() {
	sql=$1
	want=$2
	destination=$3
	# PostgreSQL credentials are expanded only inside the database container.
	# shellcheck disable=SC2016
	if printf '%s\n' "$sql" | "$kubectl_bin" exec --stdin --namespace "$namespace" \
		statefulset/vela-lab-postgres -- sh -ec \
		'PGPASSWORD=$POSTGRES_PASSWORD exec psql --no-psqlrc --quiet --set=ON_ERROR_STOP=1 --set=VERBOSITY=verbose --tuples-only --no-align --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"' \
		>"$destination" 2>&1; then
		return 1
	fi
	grep -Eq "ERROR:  $want:" "$destination"
}

fixture_count() {
	query_database "
SELECT
  (SELECT count(*) FROM customer_organizations WHERE id='$other_organization_id'::uuid) +
  (SELECT count(*) FROM projects WHERE id IN ('$same_org_project_id'::uuid,'$other_org_project_id'::uuid)) +
  (SELECT count(*) FROM principals WHERE id IN ('$same_org_principal_id'::uuid,'$other_org_principal_id'::uuid)) +
  (SELECT count(*) FROM service_principals WHERE principal_id IN ('$same_org_principal_id'::uuid,'$other_org_principal_id'::uuid)) +
  (SELECT count(*) FROM project_principal_attributions
    WHERE principal_id IN ('$same_org_principal_id'::uuid,'$other_org_principal_id'::uuid)) +
  (SELECT count(*) FROM credentials WHERE id IN ('$probe_credential_id'::uuid,'$invalid_credential_id'::uuid)) +
  (SELECT count(*) FROM project_actor_session_attributions
    WHERE actor_session_id IN ('$probe_credential_id'::uuid,'$invalid_credential_id'::uuid)) +
  (SELECT count(*) FROM jobs WHERE id IN ('$same_org_job_id'::uuid,'$other_org_job_id'::uuid)) +
  (SELECT count(*) FROM retry_runtime_states WHERE job_id IN ('$same_org_job_id'::uuid,'$other_org_job_id'::uuid)) +
  (SELECT count(*) FROM attempts WHERE id IN ('$same_org_attempt_id'::uuid,'$other_org_attempt_id'::uuid)) +
  (SELECT count(*) FROM artifacts WHERE id IN (
    '$same_org_video_id'::uuid,'$same_org_thumbnail_id'::uuid,
    '$other_org_video_id'::uuid,'$other_org_thumbnail_id'::uuid)) +
  (SELECT count(*) FROM artifact_sets WHERE id IN ('$same_org_artifact_set_id'::uuid,'$other_org_artifact_set_id'::uuid)) +
  (SELECT count(*) FROM artifact_set_items
    WHERE artifact_set_id IN ('$same_org_artifact_set_id'::uuid,'$other_org_artifact_set_id'::uuid)) +
  (SELECT count(*) FROM artifact_access_grants WHERE id IN ('$same_org_grant_id'::uuid,'$other_org_grant_id'::uuid));"
}

write_database_snapshot() {
	destination=$1
	fixture_rows=$(fixture_count)
	query_database "
SELECT jsonb_build_object(
  'active_jobs',(SELECT count(*) FROM jobs WHERE state IN ('QUEUED','ASSIGNED','RUNNING','FINALIZING','RETRY_WAIT','CANCELING')),
  'active_leases',(SELECT count(*) FROM attempt_leases WHERE revoked_at IS NULL),
  'production_gate_receipts',(SELECT count(*) FROM production_gate_receipts),
  'ready_healthy_workers',(SELECT count(*) FROM workers WHERE lifecycle_state='READY' AND reachability_condition='HEALTHY'),
  'active_break_glass_grants',(SELECT count(*) FROM break_glass_grants WHERE revoked_at IS NULL AND expires_at>clock_timestamp()),
  'actor_session_attributions',(SELECT count(*) FROM project_actor_session_attributions),
  'fixture_rows',$fixture_rows
)::text;" >"$destination"
}

write_surface_role_snapshot() {
	expected_database_surfaces >"$temporary/expected-database-surfaces.txt"
	$kubectl_bin get deployment --namespace "$namespace" vela-lab-control -o json \
		>"$temporary/control-deployment-runtime.json"
	$kubectl_bin get pods --namespace "$namespace" \
		--selector app.kubernetes.io/name=vela-lab-control -o json \
		>"$temporary/control-pods-runtime.json"
	jq -e '
	  .spec.replicas == 1
	  and ([.spec.template.spec.containers[] | select(.name == "control")] | length) == 1
	  and ([.spec.template.spec.containers[] | select(.name == "control")
	    | .envFrom[]? | select(.secretRef.name == "vela-lab-control-database-env")] | length) == 1
	' "$temporary/control-deployment-runtime.json" >/dev/null ||
		fail "control Deployment database Secret binding is invalid"
	jq -e '
	  [.items[] | select(.status.phase == "Running")
	    | select(all(.status.containerStatuses[]?; .ready == true))] | length == 1
	' "$temporary/control-pods-runtime.json" >/dev/null || fail "control Pod runtime identity is not unique and Ready"

	database_secret_json=$($kubectl_bin get secret --namespace "$namespace" \
		vela-lab-control-database-env -o json)
	expected_keys=$(cut -d '|' -f 1 "$temporary/expected-database-surfaces.txt" | sort)
	observed_keys=$(printf '%s' "$database_secret_json" | jq -r '.data | keys[]' | sort)
	[ "$observed_keys" = "$expected_keys" ] || fail "control database Secret key inventory drifted"
	: >"$temporary/surface-role-bindings.ndjson"
	while IFS='|' read -r environment role; do
		encoded=$(printf '%s' "$database_secret_json" | jq -er --arg environment "$environment" '.data[$environment]')
		database_url=$(printf '%s' "$encoded" | base64 -d)
		case "$database_url" in postgres://* | postgresql://*) ;; *) fail "$environment is not a PostgreSQL URL" ;; esac
		authority=${database_url#*://}
		userinfo=${authority%%@*}
		[ "$userinfo" != "$authority" ] || fail "$environment PostgreSQL URL omits user information"
		username=${userinfo%%:*}
		[ "$username" = "${role}_login" ] || fail "$environment is not bound to ${role}_login"
		jq -cn --arg environment "$environment" --arg role "$role" --arg login "$username" \
			'{environment:$environment,role:$role,login:$login}' >>"$temporary/surface-role-bindings.ndjson"
	done <"$temporary/expected-database-surfaces.txt"
	jq -s '.' "$temporary/surface-role-bindings.ndjson" >"$temporary/surface-role-bindings.json"
	rm "$temporary/surface-role-bindings.ndjson"

	deployment_uid=$(jq -er '.metadata.uid' "$temporary/control-deployment-runtime.json")
	deployment_image=$(jq -er '.spec.template.spec.containers[] | select(.name == "control") | .image' \
		"$temporary/control-deployment-runtime.json")
	pod_uid=$(jq -er '.items[] | select(.status.phase == "Running") | .metadata.uid' \
		"$temporary/control-pods-runtime.json")
	pod_image_id=$(jq -er '.items[] | select(.status.phase == "Running")
	  | .status.containerStatuses[] | select(.name == "control") | .imageID' \
		"$temporary/control-pods-runtime.json")
	secret_uid=$(printf '%s' "$database_secret_json" | jq -er '.metadata.uid')
	secret_resource_version=$(printf '%s' "$database_secret_json" | jq -er '.metadata.resourceVersion')
	jq -n \
		--arg deployment_uid "$deployment_uid" \
		--arg deployment_image "$deployment_image" \
		--arg pod_uid "$pod_uid" \
		--arg pod_image_id "$pod_image_id" \
		--arg secret_uid "$secret_uid" \
		--arg secret_resource_version "$secret_resource_version" \
		--slurpfile bindings "$temporary/surface-role-bindings.json" '
	{
	  schema:"vela-lab-surface-role-snapshot-v2",
	  evidence_boundary:"NON_PRODUCTION_MOCK_REHEARSAL",
	  deployment:{name:"vela-lab-control",uid:$deployment_uid,image:$deployment_image},
	  pod:{uid:$pod_uid,image_id:$pod_image_id},
	  database_secret:{name:"vela-lab-control-database-env",uid:$secret_uid,
	    resource_version:$secret_resource_version},
	  bindings:$bindings[0],
	  configured_public_http_pools:[
	    {surface:"service-credential-authentication",environment:"VELA_AUTH_DATABASE_URL",role:"vela_auth",login:"vela_auth_login"},
	    {surface:"project-job-submit-and-read",environment:"VELA_REQUEST_DATABASE_URL",role:"vela_request",login:"vela_request_login"},
	    {surface:"project-artifact-read",environment:"VELA_ARTIFACT_REQUEST_DATABASE_URL",role:"vela_artifact_request",login:"vela_artifact_request_login"}
	  ]
	}' >"$temporary/surface-role-snapshot.json"
	digest=${deployment_image##*@}
	jq -e --arg digest "$digest" '
	  .schema == "vela-lab-surface-role-snapshot-v2"
	  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
	  and (.bindings | length) == 30
	  and ([.bindings[].environment] | unique | length) == 30
	  and ([.bindings[].login] | unique | length) == 30
	  and all(.bindings[]; .login == (.role + "_login"))
	  and (.configured_public_http_pools | length) == 3
	  and (. as $root | all(.configured_public_http_pools[];
	    . as $public | any($root.bindings[]; .environment == $public.environment
	      and .role == $public.role and .login == $public.login)))
	  and (.pod.image_id == $digest or (.pod.image_id | endswith("@" + $digest)))
	' "$temporary/surface-role-snapshot.json" >/dev/null ||
		fail "configured database pool snapshot is invalid"
}

cleanup_fixture() {
	[ "$fixture_owned" = true ] || return 0
	query_database "
BEGIN;
SET LOCAL session_replication_role=replica;
DELETE FROM artifact_set_items WHERE artifact_set_id IN ('$same_org_artifact_set_id'::uuid,'$other_org_artifact_set_id'::uuid);
DELETE FROM artifact_access_grants WHERE id IN ('$same_org_grant_id'::uuid,'$other_org_grant_id'::uuid);
DELETE FROM artifact_sets WHERE id IN ('$same_org_artifact_set_id'::uuid,'$other_org_artifact_set_id'::uuid);
DELETE FROM artifacts WHERE id IN (
  '$same_org_video_id'::uuid,'$same_org_thumbnail_id'::uuid,
  '$other_org_video_id'::uuid,'$other_org_thumbnail_id'::uuid);
DELETE FROM attempts WHERE id IN ('$same_org_attempt_id'::uuid,'$other_org_attempt_id'::uuid);
DELETE FROM retry_runtime_states WHERE job_id IN ('$same_org_job_id'::uuid,'$other_org_job_id'::uuid);
DELETE FROM jobs WHERE id IN ('$same_org_job_id'::uuid,'$other_org_job_id'::uuid);
DELETE FROM credentials WHERE id IN ('$probe_credential_id'::uuid,'$invalid_credential_id'::uuid);
DELETE FROM project_principal_attributions
  WHERE principal_id IN ('$same_org_principal_id'::uuid,'$other_org_principal_id'::uuid);
DELETE FROM service_principals WHERE principal_id IN ('$same_org_principal_id'::uuid,'$other_org_principal_id'::uuid);
DELETE FROM principals WHERE id IN ('$same_org_principal_id'::uuid,'$other_org_principal_id'::uuid);
DELETE FROM projects WHERE id IN ('$same_org_project_id'::uuid,'$other_org_project_id'::uuid);
DELETE FROM customer_organizations WHERE id='$other_organization_id'::uuid;
COMMIT;" >/dev/null
	[ "$(fixture_count)" = 0 ] || return 1
	fixture_owned=false
}

delete_owned_resource() {
	kind=$1
	name=$2
	uid=$3
	[ -n "$uid" ] || return 0
	case "$kind" in
		pods)
			resource_kind=pod
			endpoint="/api/v1/namespaces/$namespace/pods/$name"
			;;
		configmaps)
			resource_kind=configmap
			endpoint="/api/v1/namespaces/$namespace/configmaps/$name"
			;;
		*) return 1 ;;
	esac
	observed=$($kubectl_bin get --namespace "$namespace" "$resource_kind/$name" \
		--ignore-not-found -o jsonpath='{.metadata.uid}') || return 1
	[ -z "$observed" ] && return 0
	[ "$observed" = "$uid" ] || return 1
	jq -n --arg uid "$uid" \
		'{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid},propagationPolicy:"Background"}' |
		$kubectl_bin delete --raw="$endpoint" -f - >/dev/null || return 1
	iteration=0
	while [ "$iteration" -lt 60 ]; do
		observed=$($kubectl_bin get --namespace "$namespace" "$resource_kind/$name" \
			--ignore-not-found -o jsonpath='{.metadata.uid}') || return 1
		[ -z "$observed" ] && return 0
		[ "$observed" = "$uid" ] || return 1
		iteration=$((iteration + 1))
		sleep 1
	done
	return 1
}

cleanup() {
	status=$?
	trap - EXIT HUP INT TERM
	if [ "$committed" != true ] && [ -n "$temporary" ] && [ -d "$temporary" ]; then
		printf 'status=INCOMPLETE evidence_boundary=NON_PRODUCTION_MOCK_REHEARSAL production_gates=0/9\n' \
			>"$temporary/STATUS"
		cleanup_result=0
		delete_owned_resource pods "$probe_name" "$probe_uid" >>"$temporary/cleanup.log" 2>&1 || cleanup_result=1
		delete_owned_resource configmaps "$probe_configmap" "$configmap_uid" >>"$temporary/cleanup.log" 2>&1 || cleanup_result=1
		cleanup_fixture >>"$temporary/cleanup.log" 2>&1 || cleanup_result=1
		if [ "$cleanup_result" -ne 0 ]; then
			printf 'organization-isolation-content-safety: cleanup incomplete\n' >&2
		fi
		printf 'organization-isolation-content-safety: diagnostic receipt preserved at %s\n' \
			"$temporary" >&2
	fi
	exit "$status"
}
trap cleanup EXIT HUP INT TERM

[ "$apply" = --apply ] ||
	fail "usage: $0 <probe.py> <runner-image@sha256:digest> <new-output-directory> --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root to read the RKE2 kubeconfig"
[ "$(hostname)" = "$control_host" ] || fail "run only on the lab control host"
[ -x "$kubectl_bin" ] || fail "$kubectl_bin is missing"
[ -r "$kubeconfig" ] || fail "$kubeconfig is unreadable"
export KUBECONFIG="$kubeconfig"
case "$probe_source" in /*) ;; *) fail "probe source path must be absolute" ;; esac
case "$output" in /*) ;; *) fail "output path must be absolute" ;; esac
[ -f "$probe_source" ] && [ ! -L "$probe_source" ] || fail "probe source is missing or unsafe"
[ "$runner_image" = "$expected_runner_image" ] || fail "Runner image does not match the pinned digest"
[ ! -e "$output" ] && [ ! -L "$output" ] || fail "output path already exists"
for command in base64 cut jq sha256sum stat; do command -v "$command" >/dev/null 2>&1 || fail "$command is required"; done

parent=$(dirname "$output")
[ -d "$parent" ] && [ ! -L "$parent" ] || fail "output parent is missing or unsafe"
temporary=$(mktemp -d "$parent/.organization-isolation-content-safety.XXXXXX")
chmod 0700 "$temporary"

for resource in "pod/$probe_name" "configmap/$probe_configmap"; do
	if $kubectl_bin get --namespace "$namespace" "$resource" >/dev/null 2>&1; then
		fail "temporary resource $resource already exists"
	fi
done
[ "$(fixture_count)" = 0 ] || fail "synthetic fixture IDs already exist"
fixture_rows=$(fixture_count)

write_database_snapshot "$temporary/database-before.json"
jq -e '
  .active_jobs == 0 and .active_leases == 0 and .production_gate_receipts == 0
  and .ready_healthy_workers == 2 and .active_break_glass_grants == 0 and .fixture_rows == 0
' "$temporary/database-before.json" >/dev/null || fail "database is not at the idle two-Worker boundary"

write_surface_role_snapshot
query_database "
WITH login_roles AS (
  SELECT role.* FROM pg_roles role WHERE role.rolname LIKE 'vela\\_%\\_login' ESCAPE '\\'
), login_memberships AS (
  SELECT login.rolname AS login,
    COALESCE(jsonb_agg(group_role.rolname ORDER BY group_role.rolname)
      FILTER (WHERE group_role.rolname IS NOT NULL),'[]'::jsonb) AS roles
  FROM login_roles login
  LEFT JOIN pg_auth_members membership ON membership.member=login.oid
  LEFT JOIN pg_roles group_role ON group_role.oid=membership.roleid
  GROUP BY login.rolname
), selected_groups AS (
  SELECT DISTINCT group_role.*
  FROM pg_roles group_role
  JOIN login_memberships membership
    ON group_role.rolname=left(membership.login,length(membership.login)-length('_login'))
), group_memberships AS (
  SELECT selected.rolname AS role,
    COALESCE(jsonb_agg(parent.rolname ORDER BY parent.rolname)
      FILTER (WHERE parent.rolname IS NOT NULL),'[]'::jsonb) AS roles
  FROM selected_groups selected
  LEFT JOIN pg_auth_members membership ON membership.member=selected.oid
  LEFT JOIN pg_roles parent ON parent.oid=membership.roleid
  GROUP BY selected.rolname
), direct_login_grants AS (
  SELECT role.rolname AS login,
    'relation:'||namespace.nspname||'.'||object.relname||':'||acl.privilege_type AS grant
  FROM pg_class object
  JOIN pg_namespace namespace ON namespace.oid=object.relnamespace
  CROSS JOIN LATERAL aclexplode(object.relacl) acl
  JOIN login_roles role ON role.oid=acl.grantee
  UNION ALL
  SELECT role.rolname,
    'column:'||namespace.nspname||'.'||object.relname||'.'||attribute.attname||':'||
      acl.privilege_type
  FROM pg_class object
  JOIN pg_namespace namespace ON namespace.oid=object.relnamespace
  JOIN pg_attribute attribute ON attribute.attrelid=object.oid
  CROSS JOIN LATERAL aclexplode(attribute.attacl) acl
  JOIN login_roles role ON role.oid=acl.grantee
  UNION ALL
  SELECT role.rolname,
    'routine:'||namespace.nspname||'.'||object.proname||':'||acl.privilege_type
  FROM pg_proc object
  JOIN pg_namespace namespace ON namespace.oid=object.pronamespace
  CROSS JOIN LATERAL aclexplode(object.proacl) acl
  JOIN login_roles role ON role.oid=acl.grantee
  UNION ALL
  SELECT role.rolname,'schema:'||object.nspname||':'||acl.privilege_type
  FROM pg_namespace object
  CROSS JOIN LATERAL aclexplode(object.nspacl) acl
  JOIN login_roles role ON role.oid=acl.grantee
  UNION ALL
  SELECT role.rolname,'database:'||object.datname||':'||acl.privilege_type
  FROM pg_database object
  CROSS JOIN LATERAL aclexplode(object.datacl) acl
  JOIN login_roles role ON role.oid=acl.grantee
), public_roles(role) AS (
  VALUES ('vela_auth'),('vela_request'),('vela_artifact_request')
), direct_table_privileges AS (
  SELECT grant_row.grantee AS role,grant_row.table_name,grant_row.privilege_type
  FROM information_schema.role_table_grants grant_row
  JOIN public_roles selected ON selected.role=grant_row.grantee
  WHERE grant_row.table_schema='public'
), effective_table_privileges AS (
  SELECT selected.role,object.relname AS table_name,privilege.name AS privilege_type
  FROM public_roles selected
  CROSS JOIN pg_class object
  JOIN pg_namespace namespace ON namespace.oid=object.relnamespace
  CROSS JOIN (VALUES ('SELECT'),('INSERT'),('UPDATE'),('DELETE'),('TRUNCATE'),
    ('REFERENCES'),('TRIGGER')) privilege(name)
  WHERE namespace.nspname='public' AND object.relkind IN ('r','p','v','m','f')
    AND has_table_privilege(selected.role,object.oid,privilege.name)
), effective_column_only_privileges AS (
  SELECT selected.role,object.relname AS table_name,attribute.attname AS column_name,
    privilege.name AS privilege_type
  FROM public_roles selected
  CROSS JOIN pg_class object
  JOIN pg_namespace namespace ON namespace.oid=object.relnamespace
  JOIN pg_attribute attribute ON attribute.attrelid=object.oid
    AND attribute.attnum>0 AND NOT attribute.attisdropped
  CROSS JOIN (VALUES ('SELECT'),('INSERT'),('UPDATE'),('REFERENCES')) privilege(name)
  WHERE namespace.nspname='public' AND object.relkind IN ('r','p','v','m','f')
    AND has_column_privilege(selected.role,object.oid,attribute.attnum,privilege.name)
    AND NOT has_table_privilege(selected.role,object.oid,privilege.name)
), direct_column_privileges AS (
  SELECT selected.role,object.relname AS table_name,attribute.attname AS column_name,
    acl.privilege_type
  FROM public_roles selected
  JOIN pg_roles selected_role ON selected_role.rolname=selected.role
  CROSS JOIN pg_class object
  JOIN pg_namespace namespace ON namespace.oid=object.relnamespace
  JOIN pg_attribute attribute ON attribute.attrelid=object.oid
  CROSS JOIN LATERAL aclexplode(attribute.attacl) acl
  WHERE namespace.nspname='public' AND acl.grantee=selected_role.oid
), direct_routine_privileges AS (
  SELECT grant_row.grantee AS role,grant_row.routine_schema,grant_row.routine_name,
    grant_row.privilege_type
  FROM information_schema.role_routine_grants grant_row
  JOIN public_roles selected ON selected.role=grant_row.grantee
  WHERE grant_row.routine_schema='public'
), effective_routine_privileges AS (
  SELECT selected.role,namespace.nspname AS routine_schema,object.proname AS routine_name,
    'EXECUTE'::text AS privilege_type
  FROM public_roles selected
  CROSS JOIN pg_proc object
  JOIN pg_namespace namespace ON namespace.oid=object.pronamespace
  WHERE namespace.nspname='public'
    AND has_function_privilege(selected.role,object.oid,'EXECUTE')
), sequence_privileges AS (
  SELECT selected.role,sequence.relname AS sequence,privilege.name AS privilege
  FROM public_roles selected
  CROSS JOIN pg_class sequence
  JOIN pg_namespace namespace ON namespace.oid=sequence.relnamespace
  CROSS JOIN (VALUES ('SELECT'),('USAGE'),('UPDATE')) privilege(name)
  WHERE sequence.relkind='S' AND namespace.nspname='public'
    AND has_sequence_privilege(selected.role,sequence.oid,privilege.name)
), unsafe_schema_privileges AS (
  SELECT selected.role,namespace.nspname AS schema
  FROM public_roles selected CROSS JOIN pg_namespace namespace
  WHERE has_schema_privilege(selected.role,namespace.oid,'CREATE')
), connected_logins AS (
  SELECT activity.usename AS login,count(*) AS sessions
  FROM pg_stat_activity activity JOIN login_roles role ON role.rolname=activity.usename
  WHERE activity.datname=current_database()
  GROUP BY activity.usename
)
SELECT jsonb_build_object(
  'schema','vela-lab-database-role-snapshot-v3',
  'roles',(SELECT jsonb_agg(jsonb_build_object(
    'name',role.rolname,'login',role.rolcanlogin,'superuser',role.rolsuper,
    'create_database',role.rolcreatedb,'create_role',role.rolcreaterole,
    'replication',role.rolreplication,'bypass_rls',role.rolbypassrls) ORDER BY role.rolname)
    FROM (SELECT * FROM login_roles UNION ALL SELECT * FROM selected_groups) role),
  'login_memberships',(SELECT jsonb_agg(to_jsonb(membership) ORDER BY membership.login)
    FROM login_memberships membership),
  'group_memberships',(SELECT jsonb_agg(to_jsonb(membership) ORDER BY membership.role)
    FROM group_memberships membership),
  'direct_login_grants',COALESCE((SELECT jsonb_agg(to_jsonb(grant_row) ORDER BY grant_row.login,grant_row.grant)
    FROM direct_login_grants grant_row),'[]'::jsonb),
  'direct_public_table_privileges',COALESCE((SELECT jsonb_agg(to_jsonb(grant_row)
    ORDER BY grant_row.role,grant_row.table_name,grant_row.privilege_type)
    FROM direct_table_privileges grant_row),'[]'::jsonb),
  'effective_public_table_privileges',COALESCE((SELECT jsonb_agg(to_jsonb(grant_row)
    ORDER BY grant_row.role,grant_row.table_name,grant_row.privilege_type)
    FROM effective_table_privileges grant_row),'[]'::jsonb),
  'effective_public_column_only_privileges',COALESCE((SELECT jsonb_agg(to_jsonb(grant_row)
    ORDER BY grant_row.role,grant_row.table_name,grant_row.column_name,grant_row.privilege_type)
    FROM effective_column_only_privileges grant_row),'[]'::jsonb),
  'direct_public_column_privileges',COALESCE((SELECT jsonb_agg(to_jsonb(grant_row)
    ORDER BY grant_row.role,grant_row.table_name,grant_row.column_name,grant_row.privilege_type)
    FROM direct_column_privileges grant_row),'[]'::jsonb),
  'direct_public_routine_privileges',COALESCE((SELECT jsonb_agg(to_jsonb(grant_row)
    ORDER BY grant_row.role,grant_row.routine_schema,grant_row.routine_name,grant_row.privilege_type)
    FROM direct_routine_privileges grant_row),'[]'::jsonb),
  'effective_public_routine_privileges',COALESCE((SELECT jsonb_agg(to_jsonb(grant_row)
    ORDER BY grant_row.role,grant_row.routine_schema,grant_row.routine_name,grant_row.privilege_type)
    FROM effective_routine_privileges grant_row),'[]'::jsonb),
  'public_sequence_privileges',COALESCE((SELECT jsonb_agg(to_jsonb(grant_row)
    ORDER BY grant_row.role,grant_row.sequence,grant_row.privilege)
    FROM sequence_privileges grant_row),'[]'::jsonb),
  'unsafe_schema_privileges',COALESCE((SELECT jsonb_agg(to_jsonb(grant_row)
    ORDER BY grant_row.role,grant_row.schema) FROM unsafe_schema_privileges grant_row),'[]'::jsonb),
  'connected_logins',COALESCE((SELECT jsonb_agg(to_jsonb(connection) ORDER BY connection.login)
    FROM connected_logins connection),'[]'::jsonb)
)::text;" >"$temporary/role-snapshot.json"
jq -e --slurpfile surface "$temporary/surface-role-snapshot.json" '
  def expected_logins: [$surface[0].bindings[].login] | sort;
  def expected_groups: [$surface[0].bindings[].role] | sort;
  def expected_roles: (expected_logins + expected_groups) | sort;
  def table_grants: [
    "vela_artifact_request|artifact_access_grants|SELECT",
    "vela_artifact_request|artifact_set_items|SELECT",
    "vela_artifact_request|artifact_sets|SELECT",
    "vela_artifact_request|jobs|SELECT",
    "vela_request|credit_reservations|INSERT",
    "vela_request|credit_reservations|SELECT",
    "vela_request|customer_organizations|SELECT",
    "vela_request|idempotency_results|INSERT",
    "vela_request|idempotency_results|SELECT",
    "vela_request|jobs|INSERT",
    "vela_request|jobs|SELECT",
    "vela_request|organization_credit_accounts|SELECT",
    "vela_request|outbox_events|INSERT",
    "vela_request|outbox_events|SELECT",
    "vela_request|principals|SELECT",
    "vela_request|projects|SELECT",
    "vela_request|retry_runtime_states|INSERT",
    "vela_request|service_principals|SELECT",
    "vela_request|vela_request_execution_lease_renewal_protocol|SELECT",
    "vela_request|vela_request_job_progress|SELECT",
    "vela_request|vela_request_job_runtime|SELECT",
    "vela_request|worker_pools|SELECT"
  ] | sort;
  def routine_grants: [
    "vela_artifact_request|public|vela_current_organization_id|EXECUTE",
    "vela_artifact_request|public|vela_current_principal_id|EXECUTE",
    "vela_artifact_request|public|vela_current_project_id|EXECUTE",
    "vela_artifact_request|public|vela_current_request_scope|EXECUTE",
    "vela_artifact_request|public|vela_set_artifact_request_context|EXECUTE",
    "vela_auth|public|vela_authenticate_service_credential|EXECUTE",
    "vela_request|public|vela_current_organization_id|EXECUTE",
    "vela_request|public|vela_current_principal_id|EXECUTE",
    "vela_request|public|vela_current_project_id|EXECUTE",
    "vela_request|public|vela_current_request_scope|EXECUTE",
    "vela_request|public|vela_lock_compatible_pool|EXECUTE",
    "vela_request|public|vela_resolve_active_sku|EXECUTE",
    "vela_request|public|vela_set_request_context|EXECUTE"
  ] | sort;
  def column_grants: [
    "vela_request|organization_credit_accounts|reserved_minor|UPDATE",
    "vela_request|organization_credit_accounts|updated_at|UPDATE",
    "vela_request|organization_credit_accounts|version|UPDATE",
    "vela_request|projects|queued_count|UPDATE",
    "vela_request|projects|running_count|UPDATE",
    "vela_request|worker_pools|queued_count|UPDATE"
  ] | sort;
  .schema == "vela-lab-database-role-snapshot-v3"
  and ([.roles[].name] | sort) == expected_roles
  and all(.roles[]; .superuser == false and .create_database == false
    and .create_role == false and .replication == false)
  and all(.roles[] | select(.name | endswith("_login")); .login == true)
  and all(.roles[] | select(.name | endswith("_login") | not); .login == false)
  and all(.roles[]; .bypass_rls == (.name == "vela_internal" or .name == "vela_internal_login"))
  and ([.login_memberships[].login] | sort) == expected_logins
  and all(.login_memberships[]; .roles == [(.login | sub("_login$";""))])
  and ([.group_memberships[].role] | sort) == expected_groups
  and all(.group_memberships[]; .roles == [])
  and .direct_login_grants == []
  and ([.direct_public_table_privileges[]
    | [.role,.table_name,.privilege_type] | join("|")] | sort) == table_grants
  and ([.effective_public_table_privileges[]
    | [.role,.table_name,.privilege_type] | join("|")] | sort) == table_grants
  and ([.direct_public_column_privileges[]
    | [.role,.table_name,.column_name,.privilege_type] | join("|")] | sort) == column_grants
  and ([.effective_public_column_only_privileges[]
    | [.role,.table_name,.column_name,.privilege_type] | join("|")] | sort) == column_grants
  and ([.direct_public_routine_privileges[]
    | [.role,.routine_schema,.routine_name,.privilege_type] | join("|")] | sort) == routine_grants
  and ([.effective_public_routine_privileges[]
    | [.role,.routine_schema,.routine_name,.privilege_type] | join("|")] | sort) == routine_grants
  and .public_sequence_privileges == []
  and .unsafe_schema_privileges == []
  and (. as $root | all(["vela_auth_login","vela_request_login","vela_artifact_request_login"][];
    . as $login | any($root.connected_logins[]; .login == $login and .sessions > 0)))
' "$temporary/role-snapshot.json" >/dev/null || fail "database role or object-privilege snapshot is unsafe"

query_database "
WITH organization_scoped_relations AS (
  SELECT relation.relname AS name,relation.relrowsecurity AS rls_enabled,
    relation.relforcerowsecurity AS rls_forced
  FROM pg_class relation JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
  WHERE namespace.nspname='public' AND relation.relkind='r'
    AND EXISTS (SELECT 1 FROM pg_attribute attribute
      WHERE attribute.attrelid=relation.oid AND attribute.attname='organization_id'
        AND attribute.attnum>0 AND NOT attribute.attisdropped)
)
SELECT jsonb_build_object(
  'schema','vela-lab-forced-rls-inventory-v2',
  'organization_scoped_relation_count',count(*),
  'unprotected_count',count(*) FILTER (WHERE NOT rls_enabled OR NOT rls_forced),
  'relations',jsonb_agg(to_jsonb(organization_scoped_relations) ORDER BY name)
)::text FROM organization_scoped_relations;" >"$temporary/forced-rls-inventory.json"
jq -e '
  .schema == "vela-lab-forced-rls-inventory-v2"
  and .organization_scoped_relation_count > 0
  and .organization_scoped_relation_count == (.relations | length)
  and .unprotected_count == 0
  and all(.relations[]; .rls_enabled == true and .rls_forced == true)
' "$temporary/forced-rls-inventory.json" >/dev/null ||
	fail "one or more Organization-scoped relations do not enforce forced RLS"

fixture_owned=true
query_database "
BEGIN;
SET LOCAL session_replication_role=replica;
INSERT INTO customer_organizations(id,display_name) VALUES
  ('$other_organization_id'::uuid,'Synthetic Isolation Organization B');
INSERT INTO projects(id,organization_id,display_name,queued_limit,running_limit) VALUES
  ('$same_org_project_id'::uuid,'$own_organization_id'::uuid,'Synthetic Same Organization Project',1,1),
  ('$other_org_project_id'::uuid,'$other_organization_id'::uuid,'Synthetic Other Organization Project',1,1);
INSERT INTO principals(id,organization_id,kind,display_name) VALUES
  ('$same_org_principal_id'::uuid,'$own_organization_id'::uuid,'SERVICE','Synthetic Same Organization Principal'),
  ('$other_org_principal_id'::uuid,'$other_organization_id'::uuid,'SERVICE','Synthetic Other Organization Principal');
INSERT INTO service_principals(principal_id,organization_id,project_id) VALUES
  ('$same_org_principal_id'::uuid,'$own_organization_id'::uuid,'$same_org_project_id'::uuid),
  ('$other_org_principal_id'::uuid,'$other_organization_id'::uuid,'$other_org_project_id'::uuid);
INSERT INTO project_principal_attributions(organization_id,project_id,principal_id,principal_kind) VALUES
  ('$own_organization_id'::uuid,'$same_org_project_id'::uuid,'$same_org_principal_id'::uuid,'SERVICE'),
  ('$other_organization_id'::uuid,'$other_org_project_id'::uuid,'$other_org_principal_id'::uuid,'SERVICE');
COMMIT;" >/dev/null
[ "$(fixture_count)" = 9 ] || fail "synthetic identity fixture cardinality is invalid"

query_database "
BEGIN;
INSERT INTO credentials(id,organization_id,project_id,principal_id,secret_digest,scopes,expires_at,created_by_principal_id)
VALUES ('$probe_credential_id'::uuid,'$own_organization_id'::uuid,'$own_project_id'::uuid,
  '$own_principal_id'::uuid,decode(repeat('a5',32),'hex'),ARRAY['jobs:submit','jobs:read','artifacts:read'],
  clock_timestamp()+interval '1 hour','$own_principal_id'::uuid);
SET LOCAL ROLE vela_request;
SELECT count(*) FROM vela_set_request_context('$probe_credential_id'::uuid,decode(repeat('a5',32),'hex'),'jobs:submit');
WITH same_update AS (
  UPDATE projects SET queued_count=queued_count WHERE id='$same_org_project_id'::uuid RETURNING 1
), other_update AS (
  UPDATE projects SET queued_count=queued_count WHERE id='$other_org_project_id'::uuid RETURNING 1
)
SELECT jsonb_build_object(
  'phase','bound-context','own_organization_rows',(SELECT count(*) FROM customer_organizations WHERE id='$own_organization_id'::uuid),
  'other_organization_rows',(SELECT count(*) FROM customer_organizations WHERE id='$other_organization_id'::uuid),
  'own_project_rows',(SELECT count(*) FROM projects WHERE id='$own_project_id'::uuid),
  'same_org_other_project_rows',(SELECT count(*) FROM projects WHERE id='$same_org_project_id'::uuid),
  'other_org_project_rows',(SELECT count(*) FROM projects WHERE id='$other_org_project_id'::uuid),
  'same_org_update_rows',(SELECT count(*) FROM same_update),
  'other_org_update_rows',(SELECT count(*) FROM other_update)
)::text;
SELECT set_config('vela.organization_id','$other_organization_id',true),
  set_config('vela.project_id','$other_org_project_id',true),
  set_config('vela.principal_id','$other_org_principal_id',true);
SELECT jsonb_build_object(
  'phase','forged-gucs','current_organization',vela_current_organization_id(),
  'current_project',vela_current_project_id(),
  'current_principal',vela_current_principal_id(),
  'own_project_rows',(SELECT count(*) FROM projects WHERE id='$own_project_id'::uuid),
  'same_org_other_project_rows',(SELECT count(*) FROM projects WHERE id='$same_org_project_id'::uuid),
  'other_org_project_rows',(SELECT count(*) FROM projects WHERE id='$other_org_project_id'::uuid)
)::text;
ROLLBACK;" >"$temporary/rls-probe.raw"
sed -n '/^{/p' "$temporary/rls-probe.raw" | jq -s '.' >"$temporary/rls-probe.json"
jq -e \
	--arg organization "$own_organization_id" \
	--arg project "$own_project_id" \
	--arg principal "$own_principal_id" '
  length == 2
  and .[0] == {phase:"bound-context",own_organization_rows:1,other_organization_rows:0,
    own_project_rows:1,same_org_other_project_rows:0,other_org_project_rows:0,
    same_org_update_rows:0,other_org_update_rows:0}
  and .[1].phase == "forged-gucs"
  and .[1].current_organization == $organization
  and .[1].current_project == $project
  and .[1].current_principal == $principal
  and .[1].own_project_rows == 1
  and .[1].same_org_other_project_rows == 0
  and .[1].other_org_project_rows == 0
' "$temporary/rls-probe.json" >/dev/null || fail "RLS or private request context isolation failed"

expect_sqlstate "
BEGIN;
INSERT INTO credentials(id,organization_id,project_id,principal_id,secret_digest,scopes,expires_at,created_by_principal_id)
VALUES ('$probe_credential_id'::uuid,'$own_organization_id'::uuid,'$own_project_id'::uuid,
  '$own_principal_id'::uuid,decode(repeat('a5',32),'hex'),ARRAY['jobs:read'],
  clock_timestamp()+interval '1 hour','$own_principal_id'::uuid);
UPDATE credentials SET revoked_at=clock_timestamp() WHERE id='$probe_credential_id'::uuid;
SET LOCAL ROLE vela_request;
SELECT * FROM vela_set_request_context('$probe_credential_id'::uuid,decode(repeat('a5',32),'hex'),'jobs:read');" \
	28000 "$temporary/credential-revocation.log" || fail "revoked credential established a request context"

expect_sqlstate "
BEGIN;
INSERT INTO credentials(id,organization_id,project_id,principal_id,secret_digest,scopes,expires_at,created_by_principal_id)
VALUES ('$invalid_credential_id'::uuid,'$own_organization_id'::uuid,'$own_project_id'::uuid,
  '$other_org_principal_id'::uuid,decode(repeat('b6',32),'hex'),ARRAY['jobs:read'],
  clock_timestamp()+interval '1 hour','$own_principal_id'::uuid);" \
	23503 "$temporary/composite-foreign-key.log" || fail "cross-Organization composite foreign key was accepted"

jq -n '
def probes: [
  "cross-organization-organization-select-hidden",
  "cross-project-same-organization-select-hidden",
  "cross-organization-project-select-hidden",
  "cross-project-same-organization-update-hidden",
  "cross-organization-project-update-hidden",
  "forged-organization-context-rejected",
  "forged-project-context-rejected",
  "forged-principal-context-rejected",
  "revoked-credential-context-rejected",
  "cross-organization-composite-foreign-key-rejected"
];
{
  schema:"vela-lab-organization-isolation-database-probe-v1",
  status:"LAB_REHEARSAL_PASS",evidence_boundary:"NON_PRODUCTION_MOCK_REHEARSAL",
  production_gates:"0/9",negative_probe_count:(probes|length),unexpected_allow_count:0,
  negative_probes:probes,
  cross_organization_hidden:true,cross_project_hidden:true,private_context_resists_forged_gucs:true,
  credential_revocation_bypass_count:0,composite_foreign_key_rejection:true
}' >"$temporary/database-probe-summary.json"
jq -e '
  .negative_probe_count == (.negative_probes | length)
  and (.negative_probes | unique | length) == (.negative_probes | length)
' "$temporary/database-probe-summary.json" >/dev/null || fail "database negative-probe receipt is invalid"

job_id=$(query_database "
SELECT job.id FROM jobs job
JOIN artifact_access_grants grant_row ON grant_row.job_id=job.id AND grant_row.revoked_at IS NULL
WHERE job.organization_id='$own_organization_id'::uuid AND job.project_id='$own_project_id'::uuid
  AND job.state='SUCCEEDED'
  AND (SELECT count(*) FROM artifacts artifact WHERE artifact.job_id=job.id AND artifact.state='COMMITTED')=2
ORDER BY job.updated_at DESC LIMIT 1;")
printf '%s\n' "$job_id" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' ||
	fail "no exact succeeded lab Job is available for signed URL probing"
printf '%s\n' "$job_id" >"$temporary/job-id.txt"

query_database "
BEGIN;
SET LOCAL session_replication_role=replica;
CREATE FUNCTION pg_temp.clone_fixture_rows(
  p_table regclass,
  p_predicate text,
  p_replacements jsonb
) RETURNS bigint
LANGUAGE plpgsql AS \$clone\$
DECLARE
  v_columns text;
  v_values text;
  v_count bigint;
BEGIN
  SELECT
    string_agg(format('%I',attribute.attname),',' ORDER BY attribute.attnum),
    string_agg(format(
      '(jsonb_populate_record(NULL::%s,to_jsonb(source_row)||\$1)).%I',
      p_table,attribute.attname),',' ORDER BY attribute.attnum)
  INTO v_columns,v_values
  FROM pg_attribute attribute
  WHERE attribute.attrelid=p_table
    AND attribute.attnum>0
    AND NOT attribute.attisdropped
    AND attribute.attgenerated=''
    AND attribute.attidentity='';
  EXECUTE format(
    'INSERT INTO %s (%s) SELECT %s FROM %s source_row WHERE %s',
    p_table,v_columns,v_values,p_table,p_predicate)
    USING p_replacements;
  GET DIAGNOSTICS v_count=ROW_COUNT;
  RETURN v_count;
END
\$clone\$;
CREATE FUNCTION pg_temp.clone_isolation_scope(
  p_source_job uuid,
  p_organization uuid,
  p_project uuid,
  p_principal uuid,
  p_job uuid,
  p_attempt uuid,
  p_artifact_set uuid,
  p_video uuid,
  p_thumbnail uuid,
  p_grant uuid
) RETURNS void
LANGUAGE plpgsql AS \$clone\$
DECLARE
  v_source_set uuid;
  v_source_attempt uuid;
  v_source_video uuid;
  v_source_thumbnail uuid;
BEGIN
  SELECT artifact_set.id,artifact_set.attempt_id
  INTO STRICT v_source_set,v_source_attempt
  FROM artifact_sets artifact_set
  WHERE artifact_set.job_id=p_source_job;
  SELECT item.artifact_id INTO STRICT v_source_video
  FROM artifact_set_items item
  WHERE item.artifact_set_id=v_source_set AND item.kind='VIDEO';
  SELECT item.artifact_id INTO STRICT v_source_thumbnail
  FROM artifact_set_items item
  WHERE item.artifact_set_id=v_source_set AND item.kind='THUMBNAIL';

  IF pg_temp.clone_fixture_rows(
    'jobs',format('id=%L::uuid',p_source_job),
    jsonb_build_object('id',p_job,'organization_id',p_organization,
      'project_id',p_project,'created_by_principal_id',p_principal,
      'result_artifact_set_id',p_artifact_set,
      'request_content',jsonb_build_object('prompt','synthetic isolation fixture'))) <> 1 THEN
    RAISE EXCEPTION 'clone Job cardinality mismatch';
  END IF;
  IF pg_temp.clone_fixture_rows(
    'retry_runtime_states',format('job_id=%L::uuid',p_source_job),
    jsonb_build_object('job_id',p_job,'organization_id',p_organization,
      'project_id',p_project)) <> 1 THEN
    RAISE EXCEPTION 'clone RetryRuntimeState cardinality mismatch';
  END IF;
  IF pg_temp.clone_fixture_rows(
    'attempts',format('id=%L::uuid',v_source_attempt),
    jsonb_build_object('id',p_attempt,'organization_id',p_organization,
      'project_id',p_project,'job_id',p_job,
      'scheduler_dispatch_intent_id',NULL,
      'debug_dump_authorization_id',NULL,
      'debug_dump_authorization_expires_at',NULL)) <> 1 THEN
    RAISE EXCEPTION 'clone Attempt cardinality mismatch';
  END IF;
  IF pg_temp.clone_fixture_rows(
    'artifacts',format('id=%L::uuid',v_source_video),
    jsonb_build_object('id',p_video,'organization_id',p_organization,
      'project_id',p_project,'job_id',p_job,'attempt_id',p_attempt,
      'object_key','vela-lab/isolation/'||p_job::text||'/video',
      'object_version_id','synthetic-version-video')) <> 1 THEN
    RAISE EXCEPTION 'clone video Artifact cardinality mismatch';
  END IF;
  IF pg_temp.clone_fixture_rows(
    'artifacts',format('id=%L::uuid',v_source_thumbnail),
    jsonb_build_object('id',p_thumbnail,'organization_id',p_organization,
      'project_id',p_project,'job_id',p_job,'attempt_id',p_attempt,
      'object_key','vela-lab/isolation/'||p_job::text||'/thumbnail',
      'object_version_id','synthetic-version-thumbnail')) <> 1 THEN
    RAISE EXCEPTION 'clone thumbnail Artifact cardinality mismatch';
  END IF;
  IF pg_temp.clone_fixture_rows(
    'artifact_sets',format('id=%L::uuid',v_source_set),
    jsonb_build_object('id',p_artifact_set,'organization_id',p_organization,
      'project_id',p_project,'job_id',p_job,'attempt_id',p_attempt)) <> 1 THEN
    RAISE EXCEPTION 'clone ArtifactSet cardinality mismatch';
  END IF;
  IF pg_temp.clone_fixture_rows(
    'artifact_set_items',
    format('artifact_set_id=%L::uuid AND artifact_id=%L::uuid',v_source_set,v_source_video),
    jsonb_build_object('organization_id',p_organization,'project_id',p_project,
      'job_id',p_job,'attempt_id',p_attempt,'artifact_set_id',p_artifact_set,
      'artifact_id',p_video,'object_key','vela-lab/isolation/'||p_job::text||'/video',
      'object_version_id','synthetic-version-video')) <> 1 THEN
    RAISE EXCEPTION 'clone video ArtifactSet item cardinality mismatch';
  END IF;
  IF pg_temp.clone_fixture_rows(
    'artifact_set_items',
    format('artifact_set_id=%L::uuid AND artifact_id=%L::uuid',v_source_set,v_source_thumbnail),
    jsonb_build_object('organization_id',p_organization,'project_id',p_project,
      'job_id',p_job,'attempt_id',p_attempt,'artifact_set_id',p_artifact_set,
      'artifact_id',p_thumbnail,'object_key','vela-lab/isolation/'||p_job::text||'/thumbnail',
      'object_version_id','synthetic-version-thumbnail')) <> 1 THEN
    RAISE EXCEPTION 'clone thumbnail ArtifactSet item cardinality mismatch';
  END IF;
  IF pg_temp.clone_fixture_rows(
    'artifact_access_grants',format('job_id=%L::uuid',p_source_job),
    jsonb_build_object('id',p_grant,'organization_id',p_organization,
      'project_id',p_project,'job_id',p_job,'artifact_set_id',p_artifact_set)) <> 1 THEN
    RAISE EXCEPTION 'clone Artifact access grant cardinality mismatch';
  END IF;
END
\$clone\$;
SELECT pg_temp.clone_isolation_scope(
  '$job_id'::uuid,'$own_organization_id'::uuid,'$same_org_project_id'::uuid,
  '$same_org_principal_id'::uuid,'$same_org_job_id'::uuid,'$same_org_attempt_id'::uuid,
  '$same_org_artifact_set_id'::uuid,'$same_org_video_id'::uuid,'$same_org_thumbnail_id'::uuid,
  '$same_org_grant_id'::uuid);
SELECT pg_temp.clone_isolation_scope(
  '$job_id'::uuid,'$other_organization_id'::uuid,'$other_org_project_id'::uuid,
  '$other_org_principal_id'::uuid,'$other_org_job_id'::uuid,'$other_org_attempt_id'::uuid,
  '$other_org_artifact_set_id'::uuid,'$other_org_video_id'::uuid,'$other_org_thumbnail_id'::uuid,
  '$other_org_grant_id'::uuid);
COMMIT;" >/dev/null
[ "$(fixture_count)" = 27 ] || fail "synthetic foreign-resource fixture cardinality is invalid"
query_database "
WITH fixture_jobs AS (
  SELECT job.organization_id,job.project_id,job.id AS job_id,job.state,
    job.result_artifact_set_id,
    (SELECT count(*) FROM artifacts artifact WHERE artifact.job_id=job.id) AS artifact_rows,
    (SELECT count(*) FROM artifact_set_items item WHERE item.job_id=job.id) AS artifact_set_items,
    (SELECT count(*) FROM artifact_access_grants access WHERE access.job_id=job.id
      AND access.revoked_at IS NULL) AS active_access_grants
  FROM jobs job WHERE job.id IN ('$same_org_job_id'::uuid,'$other_org_job_id'::uuid)
)
SELECT jsonb_build_object(
  'schema','vela-lab-foreign-resource-fixture-v1',
  'evidence_boundary','NON_PRODUCTION_MOCK_REHEARSAL',
  'source_job_id','$job_id'::uuid,
  'jobs',jsonb_agg(to_jsonb(fixture_jobs) ORDER BY job_id)
)::text FROM fixture_jobs;" >"$temporary/foreign-resource-fixture.json"
jq -e \
	--arg same_organization "$own_organization_id" \
	--arg same_project "$same_org_project_id" \
	--arg same_job "$same_org_job_id" \
	--arg other_organization "$other_organization_id" \
	--arg other_project "$other_org_project_id" \
	--arg other_job "$other_org_job_id" '
  .schema == "vela-lab-foreign-resource-fixture-v1"
  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
  and (.jobs | length) == 2
  and all(.jobs[]; .state == "SUCCEEDED" and .result_artifact_set_id != null
    and .artifact_rows == 2 and .artifact_set_items == 2 and .active_access_grants == 1)
  and any(.jobs[]; .organization_id == $same_organization and .project_id == $same_project and .job_id == $same_job)
  and any(.jobs[]; .organization_id == $other_organization and .project_id == $other_project and .job_id == $other_job)
' "$temporary/foreign-resource-fixture.json" >/dev/null || fail "foreign-resource fixture evidence is invalid"
probe_sha256=$(sha256sum "$probe_source" | awk '{print $1}')

$kubectl_bin create configmap "$probe_configmap" --namespace "$namespace" \
	--from-file=probe.py="$probe_source" --dry-run=client -o json |
	jq --arg probe_sha256 "$probe_sha256" '
      .immutable=true
      | .metadata.labels={"app.kubernetes.io/name":"vela-lab-organization-isolation-probe",
          "app.kubernetes.io/component":"lab-rehearsal","vela.ai/environment":"non-production-lab"}
      | .metadata.annotations={"vela.ai/probe-source-sha256":$probe_sha256}
    ' >"$temporary/probe-configmap.json"
$kubectl_bin create --dry-run=server -f "$temporary/probe-configmap.json" -o json \
	>"$temporary/probe-configmap-server-dry-run.json"
$kubectl_bin create -f "$temporary/probe-configmap.json" >/dev/null
$kubectl_bin get configmap --namespace "$namespace" "$probe_configmap" -o json \
	>"$temporary/probe-configmap-runtime.json"
configmap_uid=$(jq -r '.metadata.uid' "$temporary/probe-configmap-runtime.json")
jq -e --arg uid "$configmap_uid" --arg probe_sha256 "$probe_sha256" '
  .metadata.uid == $uid and .immutable == true
  and .metadata.labels["vela.ai/environment"] == "non-production-lab"
  and .metadata.annotations["vela.ai/probe-source-sha256"] == $probe_sha256
  and (.data | keys) == ["probe.py"]
' "$temporary/probe-configmap-runtime.json" >/dev/null || fail "probe ConfigMap identity is invalid"
runtime_probe_sha256=$(jq -j '.data["probe.py"]' "$temporary/probe-configmap-runtime.json" | sha256sum | awk '{print $1}')
[ "$runtime_probe_sha256" = "$probe_sha256" ] || fail "probe ConfigMap bytes do not match the local source"

jq -n \
	--arg image "$runner_image" \
	--arg own_project "$own_project_id" \
		--arg same_org_project "$same_org_project_id" \
		--arg other_org_project "$other_org_project_id" \
		--arg job_id "$job_id" \
		--arg same_org_job_id "$same_org_job_id" \
		--arg other_org_job_id "$other_org_job_id" \
		--arg probe_sha256 "$probe_sha256" '
{
  apiVersion:"v1",kind:"Pod",
  metadata:{name:"vela-lab-organization-isolation-probe",namespace:"vela-lab",labels:{
    "app.kubernetes.io/name":"vela-lab-organization-isolation-probe",
    "app.kubernetes.io/component":"smoke","vela.ai/environment":"non-production-lab"}},
  spec:{automountServiceAccountToken:false,restartPolicy:"Never",activeDeadlineSeconds:180,terminationGracePeriodSeconds:5,
    nodeSelector:{"kubernetes.io/hostname":"vela-lab-worker-1"},
    tolerations:[{key:"vela.ai/h3",operator:"Equal",value:"true",effect:"NoSchedule"}],
    securityContext:{runAsNonRoot:true,runAsUser:10001,runAsGroup:10001,fsGroup:10001,seccompProfile:{type:"RuntimeDefault"}},
    containers:[{name:"probe",image:$image,imagePullPolicy:"IfNotPresent",
      command:["/opt/vela/venv/bin/python","/probe/probe.py"],
      env:[
        {name:"VELA_PROBE_BASE_URL",value:"http://vela-lab-control.vela-lab.svc:8080"},
        {name:"VELA_PROBE_SIGNED_HOST",value:"vela-lab-minio.vela-lab.svc:9000"},
        {name:"VELA_PROBE_OWN_PROJECT_ID",value:$own_project},
        {name:"VELA_PROBE_SAME_ORG_PROJECT_ID",value:$same_org_project},
        {name:"VELA_PROBE_OTHER_ORG_PROJECT_ID",value:$other_org_project},
        {name:"VELA_PROBE_JOB_ID",value:$job_id},
        {name:"VELA_PROBE_SAME_ORG_JOB_ID",value:$same_org_job_id},
        {name:"VELA_PROBE_OTHER_ORG_JOB_ID",value:$other_org_job_id},
        {name:"VELA_PROBE_SOURCE_SHA256",value:$probe_sha256},
        {name:"VELA_PROBE_CREDENTIAL_FILE",value:"/credential/bearer-credential"}],
      resources:{requests:{cpu:"50m",memory:"64Mi"},limits:{cpu:"500m",memory:"256Mi"}},
      securityContext:{allowPrivilegeEscalation:false,capabilities:{drop:["ALL"]},readOnlyRootFilesystem:true},
      volumeMounts:[{name:"probe",mountPath:"/probe",readOnly:true},{name:"credential",mountPath:"/credential",readOnly:true},{name:"tmp",mountPath:"/tmp"}]}],
    volumes:[
      {name:"probe",configMap:{name:"vela-lab-organization-isolation-probe",defaultMode:292}},
      {name:"credential",secret:{secretName:"vela-lab-smoke-credential",defaultMode:256}},
      {name:"tmp",emptyDir:{}}]}}
' >"$temporary/probe-pod.json"
$kubectl_bin create --dry-run=server -f "$temporary/probe-pod.json" -o json \
	>"$temporary/probe-pod-server-dry-run.json"
$kubectl_bin create -f "$temporary/probe-pod.json" >/dev/null
probe_uid=$($kubectl_bin get pod --namespace "$namespace" "$probe_name" -o jsonpath='{.metadata.uid}')

iteration=0
probe_phase=
while [ "$iteration" -lt 180 ]; do
	probe_phase=$($kubectl_bin get pod --namespace "$namespace" "$probe_name" -o jsonpath='{.status.phase}') ||
		fail "HTTP isolation probe Pod disappeared"
	case "$probe_phase" in
		Succeeded | Failed) break ;;
	esac
	iteration=$((iteration + 1))
	sleep 1
done
if [ "$probe_phase" != Succeeded ]; then
	$kubectl_bin describe --namespace "$namespace" "pod/$probe_name" >"$temporary/probe-describe.txt" 2>&1 || true
	$kubectl_bin logs --namespace "$namespace" "$probe_name" >"$temporary/http-probe.log" 2>&1 || true
	fail "HTTP isolation probe ended in phase ${probe_phase:-UNKNOWN}"
fi
$kubectl_bin logs --namespace "$namespace" "$probe_name" >"$temporary/http-probe.log"
[ "$(wc -l <"$temporary/http-probe.log" | tr -d ' ')" = 1 ] || fail "HTTP probe emitted a non-unique receipt"
	jq -e \
		--arg job_id "$job_id" \
		--arg same_org_job_id "$same_org_job_id" \
		--arg other_org_job_id "$other_org_job_id" \
		--arg probe_sha256 "$probe_sha256" '
  select(.schema == "vela-lab-organization-isolation-http-probe-v2"
  and .status == "LAB_REHEARSAL_PASS"
  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
  and .production_gates == "0/9" and .job_id == $job_id
  and .foreign_job_ids == {
    same_organization_foreign_project:$same_org_job_id,
    foreign_organization:$other_org_job_id}
  and .probe_sha256 == $probe_sha256
  and .authorized_artifact_count == 2 and .authorized_signed_get_count == 2
  and .negative_probe_count == (.negative_probes | length)
  and .negative_probe_count == (4 + (.authorized_artifact_count * 3))
  and (.negative_probes | unique | length) == (.negative_probes | length)
  and (.negative_probes | index("same-organization-foreign-project-fixture-job-hidden")) != null
  and (.negative_probes | index("same-organization-foreign-project-fixture-artifact-set-hidden")) != null
  and (.negative_probes | index("foreign-organization-fixture-job-hidden")) != null
  and (.negative_probes | index("foreign-organization-fixture-artifact-set-hidden")) != null
  and .unexpected_allow_count == 0
  and .cross_project_hidden == true and .cross_organization_hidden == true
  and .signed_url_method_bound == true and .signed_url_path_bound == true
  and .signed_url_version_bound == true
  )
' "$temporary/http-probe.log" >"$temporary/http-probe.json" || fail "HTTP probe receipt is invalid"

$kubectl_bin get pod --namespace "$namespace" "$probe_name" -o json >"$temporary/probe-pod-runtime.json"
digest=${runner_image##*@}
jq -e --arg uid "$probe_uid" --arg image "$runner_image" --arg digest "$digest" '
  .metadata.uid == $uid and .spec.nodeName == "vela-lab-worker-1" and .status.phase == "Succeeded"
  and (.spec.containers | length) == 1 and .spec.containers[0].image == $image
  and (.status.containerStatuses | length) == 1
  and (.status.containerStatuses[0].imageID == $image
    or .status.containerStatuses[0].imageID == $digest
    or (.status.containerStatuses[0].imageID | endswith("@" + $digest)))
  and .status.containerStatuses[0].state.terminated.exitCode == 0
  and (.spec.volumes | map(select(.name == "probe" and .configMap.name == "vela-lab-organization-isolation-probe")) | length) == 1
  and (.spec.volumes | map(select(.name == "credential" and .secret.secretName == "vela-lab-smoke-credential")) | length) == 1
' "$temporary/probe-pod-runtime.json" >/dev/null || fail "HTTP probe runtime identity is invalid"

delete_owned_resource pods "$probe_name" "$probe_uid" || fail "HTTP probe Pod cleanup failed"
probe_uid=
delete_owned_resource configmaps "$probe_configmap" "$configmap_uid" || fail "HTTP probe ConfigMap cleanup failed"
configmap_uid=
cleanup_fixture || fail "synthetic database fixture cleanup failed"
write_database_snapshot "$temporary/database-after.json"
jq -e --slurpfile before "$temporary/database-before.json" '
  . == $before[0] and .active_jobs == 0 and .active_leases == 0
  and .production_gate_receipts == 0 and .ready_healthy_workers == 2
  and .active_break_glass_grants == 0 and .fixture_rows == 0
' "$temporary/database-after.json" >/dev/null || fail "database did not return to its exact idle boundary"

$kubectl_bin get configmap --namespace "$namespace" vela-lab-control-runtime -o json |
	jq '{schema:"vela-lab-missing-external-identity-v1",
      customer_oidc_issuer:.data.VELA_OIDC_ISSUER,
      platform_oidc_issuer:.data.VELA_PLATFORM_OIDC_ISSUER}' >"$temporary/external-identity-gap.json"
jq -e '
  .schema == "vela-lab-missing-external-identity-v1"
  and (.customer_oidc_issuer | endswith(".invalid/"))
  and (.platform_oidc_issuer | endswith(".invalid/"))
' "$temporary/external-identity-gap.json" >/dev/null || fail "external identity gap evidence changed"

jq -n '
def scenarios: [
  {id:"cross-organization",status:"LAB_REHEARSAL_PASS"},
  {id:"cross-project",status:"LAB_REHEARSAL_PASS"},
	  {id:"fixed-role-matrix",status:"LAB_REHEARSAL_PARTIAL_REQUEST_CORRELATION_ABSENT"},
  {id:"credential-revocation",status:"LAB_REHEARSAL_PASS"},
  {id:"signed-url-scope",status:"LAB_REHEARSAL_PASS"},
  {id:"rls",status:"LAB_REHEARSAL_PASS"},
  {id:"composite-foreign-key",status:"LAB_REHEARSAL_PASS"},
  {id:"break-glass-audit",status:"NOT_RUN_REAL_PLATFORM_IDP_ABSENT"},
  {id:"customer-content-no-reuse",status:"NOT_RUN_AUDIT_SINK_ABSENT"}
];
scenarios as $scenarios | {
	  schema:"vela-lab-organization-isolation-scenario-matrix-v2",
  evidence_boundary:"NON_PRODUCTION_MOCK_REHEARSAL",production_gates:"0/9",
  scenarios:$scenarios,
  completed:([$scenarios[] | select(.status == "LAB_REHEARSAL_PASS")] | length),
  required:($scenarios | length)
}' >"$temporary/scenario-matrix.json"

captured_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
harness_sha256=$(sha256sum "$0" | awk '{print $1}')
http_negative=$(jq -r '.negative_probe_count' "$temporary/http-probe.json")
database_negative=$(jq -r '.negative_probe_count' "$temporary/database-probe-summary.json")
scenarios_completed=$(jq -r '.completed' "$temporary/scenario-matrix.json")
scenarios_required=$(jq -r '.required' "$temporary/scenario-matrix.json")
database_binding_count=$(jq -r '.bindings | length' "$temporary/surface-role-snapshot.json")
forced_rls_relation_count=$(jq -r '.organization_scoped_relation_count' \
	"$temporary/forced-rls-inventory.json")
foreign_job_count=$(jq -r '.jobs | length' "$temporary/foreign-resource-fixture.json")
foreign_artifact_count=$(jq -r '[.jobs[].artifact_rows] | add' "$temporary/foreign-resource-fixture.json")
jq -n \
	--arg captured_at "$captured_at" \
	--arg job_id "$job_id" \
	--arg runner_image "$runner_image" \
	--arg harness_sha256 "$harness_sha256" \
	--arg probe_sha256 "$probe_sha256" \
	--argjson http_negative "$http_negative" \
	--argjson database_negative "$database_negative" \
	--argjson scenarios_completed "$scenarios_completed" \
	--argjson scenarios_required "$scenarios_required" \
		--argjson database_binding_count "$database_binding_count" \
	--argjson forced_rls_relation_count "$forced_rls_relation_count" \
	--argjson foreign_job_count "$foreign_job_count" \
	--argjson foreign_artifact_count "$foreign_artifact_count" '
{
	  schema:"vela-lab-organization-isolation-content-safety-v2",
  status:"LAB_REHEARSAL_PARTIAL",evidence_boundary:"NON_PRODUCTION_MOCK_REHEARSAL",
  production_gates:"0/9",captured_at:$captured_at,job_id:$job_id,
  runner_image:$runner_image,harness_sha256:$harness_sha256,probe_sha256:$probe_sha256,
  scenarios_completed:$scenarios_completed,scenarios_required:$scenarios_required,
  negative_probe_count:($http_negative+$database_negative),unexpected_allow_count:0,
  credential_revocation_bypass_count:0,
	  configured_database_binding_count:$database_binding_count,
	  forced_rls_relation_count:$forced_rls_relation_count,
	  foreign_resource_fixture:{jobs:$foreign_job_count,artifacts:$foreign_artifact_count},
	  request_correlated_role_measurement:{state:"NOT_MEASURED",
	    reason:"REQUEST_CORRELATION_TELEMETRY_ABSENT",count:null},
	  customer_content_reuse_measurement:{state:"NOT_MEASURED",reason:"AUDIT_SINK_ABSENT",count:null},
	  unaudited_break_glass_measurement:{state:"NOT_MEASURED",reason:"REAL_PLATFORM_IDP_ABSENT",count:null},
	  missing_dependencies:["request-correlated-database-role-telemetry","real-customer-idp",
	    "real-platform-idp","break-glass-approval-workflow","customer-content-reuse-audit-sink"]
}' >"$temporary/summary.json"
printf 'status=LAB_REHEARSAL_PARTIAL evidence_boundary=NON_PRODUCTION_MOCK_REHEARSAL production_gates=0/9 scenarios=%s/%s\n' \
	"$scenarios_completed" "$scenarios_required" >"$temporary/STATUS"
(
	cd "$temporary"
	checksum_paths=$(find . -type f ! -name SHA256SUMS -printf '%P\n' | sort)
	printf '%s\n' "$checksum_paths" |
		while IFS= read -r path; do sha256sum "$path"; done >SHA256SUMS
	sha256sum --check --strict SHA256SUMS >/dev/null
)
chmod 0700 "$temporary"
mv "$temporary" "$output"
temporary=
committed=true
trap - EXIT HUP INT TERM
printf 'schema=vela-lab-organization-isolation-content-safety-wrapper-v2 output=%s result=LAB_REHEARSAL_PARTIAL scenarios=%s/%s production_gates=0/9\n' \
	"$output" "$scenarios_completed" "$scenarios_required"
