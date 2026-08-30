#!/bin/sh

set -eu
umask 077

namespace=vela-lab
control_host=marslab-server
own_organization_id=84000000-0000-0000-0000-000000000001
own_project_id=84000000-0000-0000-0000-000000000002
own_principal_id=84000000-0000-0000-0000-000000000003
same_org_project_id=85000000-0000-0000-0000-000000000002
other_organization_id=85000000-0000-0000-0000-000000000001
other_org_project_id=85000000-0000-0000-0000-000000000003
other_org_principal_id=85000000-0000-0000-0000-000000000004
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
  (SELECT count(*) FROM principals WHERE id='$other_org_principal_id'::uuid) +
  (SELECT count(*) FROM credentials WHERE id IN ('$probe_credential_id'::uuid,'$invalid_credential_id'::uuid)) +
  (SELECT count(*) FROM project_actor_session_attributions
    WHERE actor_session_id IN ('$probe_credential_id'::uuid,'$invalid_credential_id'::uuid));"
}

cleanup_fixture() {
	[ "$fixture_owned" = true ] || return 0
	query_database "
BEGIN;
DELETE FROM credentials WHERE id IN ('$probe_credential_id'::uuid,'$invalid_credential_id'::uuid);
DELETE FROM principals WHERE id='$other_org_principal_id'::uuid;
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
for command in jq sha256sum stat; do command -v "$command" >/dev/null 2>&1 || fail "$command is required"; done

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

query_database "
SELECT jsonb_build_object(
  'active_jobs',(SELECT count(*) FROM jobs WHERE state IN ('QUEUED','ASSIGNED','RUNNING','FINALIZING','RETRY_WAIT','CANCELING')),
  'active_leases',(SELECT count(*) FROM attempt_leases WHERE revoked_at IS NULL),
  'production_gate_receipts',(SELECT count(*) FROM production_gate_receipts),
  'ready_healthy_workers',(SELECT count(*) FROM workers WHERE lifecycle_state='READY' AND reachability_condition='HEALTHY'),
  'active_break_glass_grants',(SELECT count(*) FROM break_glass_grants WHERE revoked_at IS NULL AND expires_at>clock_timestamp()),
  'actor_session_attributions',(SELECT count(*) FROM project_actor_session_attributions),
  'fixture_rows',$fixture_rows
)::text;" >"$temporary/database-before.json"
jq -e '
  .active_jobs == 0 and .active_leases == 0 and .production_gate_receipts == 0
  and .ready_healthy_workers == 2 and .active_break_glass_grants == 0 and .fixture_rows == 0
' "$temporary/database-before.json" >/dev/null || fail "database is not at the idle two-Worker boundary"

query_database "
SELECT jsonb_build_object(
  'schema','vela-lab-database-role-snapshot-v1',
  'roles',(SELECT jsonb_agg(jsonb_build_object(
    'name',role.rolname,'login',role.rolcanlogin,'superuser',role.rolsuper,'bypass_rls',role.rolbypassrls)
    ORDER BY role.rolname)
    FROM pg_roles role WHERE role.rolname IN (
      'vela_request','vela_artifact_request','vela_auth',
      'vela_request_login','vela_artifact_request_login','vela_auth_login')),
  'memberships',jsonb_build_object(
    'request',pg_has_role('vela_request_login','vela_request','member'),
    'artifact_request',pg_has_role('vela_artifact_request_login','vela_artifact_request','member'),
    'auth',pg_has_role('vela_auth_login','vela_auth','member'))
)::text;" >"$temporary/role-snapshot.json"
jq -e '
  .schema == "vela-lab-database-role-snapshot-v1"
  and ([.roles[].name] | sort) == ["vela_artifact_request","vela_artifact_request_login",
    "vela_auth","vela_auth_login","vela_request","vela_request_login"]
  and all(.roles[]; .superuser == false and .bypass_rls == false)
  and all(.roles[] | select(.name | endswith("_login") | not); .login == false)
  and all(.roles[] | select(.name | endswith("_login")); .login == true)
  and .memberships == {request:true,artifact_request:true,auth:true}
' "$temporary/role-snapshot.json" >/dev/null || fail "database request-role snapshot is unsafe"

fixture_owned=true
query_database "
BEGIN;
INSERT INTO customer_organizations(id,display_name) VALUES
  ('$other_organization_id'::uuid,'Synthetic Isolation Organization B');
INSERT INTO projects(id,organization_id,display_name,queued_limit,running_limit) VALUES
  ('$same_org_project_id'::uuid,'$own_organization_id'::uuid,'Synthetic Same Organization Project',1,1),
  ('$other_org_project_id'::uuid,'$other_organization_id'::uuid,'Synthetic Other Organization Project',1,1);
INSERT INTO principals(id,organization_id,kind,display_name) VALUES
  ('$other_org_principal_id'::uuid,'$other_organization_id'::uuid,'SERVICE','Synthetic Other Organization Principal');
COMMIT;" >/dev/null
[ "$(fixture_count)" = 4 ] || fail "synthetic fixture cardinality is invalid"

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

jq -n '{
  schema:"vela-lab-organization-isolation-database-probe-v1",
  status:"LAB_REHEARSAL_PASS",evidence_boundary:"NON_PRODUCTION_MOCK_REHEARSAL",
  production_gates:"0/9",negative_probe_count:10,unexpected_allow_count:0,
  negative_probes:[
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
  ],
  cross_organization_hidden:true,cross_project_hidden:true,private_context_resists_forged_gucs:true,
  credential_revocation_bypass_count:0,composite_foreign_key_rejection:true
}' >"$temporary/database-probe-summary.json"
jq -e '
  .negative_probe_count == 10 and (.negative_probes | length) == 10
  and (.negative_probes | unique | length) == 10
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
	--arg probe_sha256 "$probe_sha256" '
  select(.schema == "vela-lab-organization-isolation-http-probe-v1"
  and .status == "LAB_REHEARSAL_PASS"
  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
  and .production_gates == "0/9" and .job_id == $job_id
  and .probe_sha256 == $probe_sha256
  and .authorized_artifact_count == 2 and .authorized_signed_get_count == 2
  and .negative_probe_count == 10 and .unexpected_allow_count == 0
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
)::text;" >"$temporary/database-after.json"
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

jq -n '{schema:"vela-lab-organization-isolation-scenario-matrix-v1",
  evidence_boundary:"NON_PRODUCTION_MOCK_REHEARSAL",production_gates:"0/9",
  scenarios:[
    {id:"cross-organization",status:"LAB_REHEARSAL_PASS"},
    {id:"cross-project",status:"LAB_REHEARSAL_PASS"},
    {id:"fixed-role-matrix",status:"LAB_REHEARSAL_PASS"},
    {id:"credential-revocation",status:"LAB_REHEARSAL_PASS"},
    {id:"signed-url-scope",status:"LAB_REHEARSAL_PASS"},
    {id:"rls",status:"LAB_REHEARSAL_PASS"},
    {id:"composite-foreign-key",status:"LAB_REHEARSAL_PASS"},
    {id:"break-glass-audit",status:"NOT_RUN_REAL_PLATFORM_IDP_ABSENT"},
    {id:"customer-content-no-reuse",status:"NOT_RUN_AUDIT_SINK_ABSENT"}
  ],completed:7,required:9}' >"$temporary/scenario-matrix.json"

captured_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
harness_sha256=$(sha256sum "$0" | awk '{print $1}')
http_negative=$(jq -r '.negative_probe_count' "$temporary/http-probe.json")
database_negative=$(jq -r '.negative_probe_count' "$temporary/database-probe-summary.json")
jq -n \
	--arg captured_at "$captured_at" \
	--arg job_id "$job_id" \
	--arg runner_image "$runner_image" \
	--arg harness_sha256 "$harness_sha256" \
	--arg probe_sha256 "$probe_sha256" \
	--argjson http_negative "$http_negative" \
	--argjson database_negative "$database_negative" '
{
  schema:"vela-lab-organization-isolation-content-safety-v1",
  status:"LAB_REHEARSAL_PARTIAL",evidence_boundary:"NON_PRODUCTION_MOCK_REHEARSAL",
  production_gates:"0/9",captured_at:$captured_at,job_id:$job_id,
  runner_image:$runner_image,harness_sha256:$harness_sha256,probe_sha256:$probe_sha256,
  scenarios_completed:7,scenarios_required:9,
  negative_probe_count:($http_negative+$database_negative),unexpected_allow_count:0,
  credential_revocation_bypass_count:0,
  customer_content_reuse_count:{state:"NOT_MEASURED",reason:"AUDIT_SINK_ABSENT"},
  unaudited_break_glass_count:{state:"NOT_MEASURED",reason:"REAL_PLATFORM_IDP_ABSENT"},
  missing_dependencies:["real-customer-idp","real-platform-idp","break-glass-approval-workflow","customer-content-reuse-audit-sink"]
}' >"$temporary/summary.json"
printf 'status=LAB_REHEARSAL_PARTIAL evidence_boundary=NON_PRODUCTION_MOCK_REHEARSAL production_gates=0/9 scenarios=7/9\n' \
	>"$temporary/STATUS"
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
printf 'schema=vela-lab-organization-isolation-content-safety-wrapper-v1 output=%s result=LAB_REHEARSAL_PARTIAL scenarios=7/9 production_gates=0/9\n' \
	"$output"
