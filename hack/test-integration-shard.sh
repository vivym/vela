#!/bin/sh

set -eu

if [ "$#" -ne 2 ]; then
	echo "usage: $0 SHARD_INDEX SHARD_TOTAL" >&2
	exit 2
fi

shard_index=$1
shard_total=$2
case "${shard_index}:${shard_total}" in
	*[!0-9:]* | :* | *:)
		echo "shard index and total must be non-negative integers" >&2
		exit 2
		;;
esac
if [ "${shard_total}" -eq 0 ] || [ "${shard_index}" -ge "${shard_total}" ]; then
	echo "shard index must be less than a non-zero shard total" >&2
	exit 2
fi

work_dir=$(mktemp -d)
trap 'rm -rf "${work_dir}"' EXIT HUP INT TERM
package_template='{{.ImportPath}} {{join .TestGoFiles ","}} {{join .XTestGoFiles ","}}'
go list -f "${package_template}" ./... >"${work_dir}/regular"
go list -tags=integration -f "${package_template}" ./... >"${work_dir}/integration"
LC_ALL=C sort "${work_dir}/regular" >"${work_dir}/regular-sorted"
LC_ALL=C sort "${work_dir}/integration" >"${work_dir}/integration-sorted"
LC_ALL=C comm -13 "${work_dir}/regular-sorted" "${work_dir}/integration-sorted" |
	awk '{print $1}' >"${work_dir}/packages"

# Compare Go's build-selected test files so new integration packages cannot be
# silently excluded, including packages containing both unit and tagged tests.
: >"${work_dir}/tests"
while IFS= read -r package; do
	go test -tags=integration "${package}" -list '^Test' >"${work_dir}/package-tests"
	awk -v package="${package}" '/^Test[[:alnum:]_]+$/ {print package, $0}' \
		"${work_dir}/package-tests" >>"${work_dir}/tests"
done <"${work_dir}/packages"

awk -v shard_index="${shard_index}" -v shard_total="${shard_total}" '
	{
		if (test_count % shard_total == shard_index) {
			if (!($1 in patterns)) {
				packages[package_count++] = $1
			} else {
				patterns[$1] = patterns[$1] "|"
			}
			patterns[$1] = patterns[$1] $2
			shard_count++
		}
		test_count++
	}
	END {
		if (test_count == 0) {
			exit 2
		}
		for (i = 0; i < package_count; i++) {
			package = packages[i]
			printf "%s ^(%s)$\n", package, patterns[package]
		}
	}
' "${work_dir}/tests" >"${work_dir}/selected"

echo "running integration shard ${shard_index}/${shard_total}"
while IFS=' ' read -r package test_pattern; do
	go test -tags=integration "${package}" -count=1 \
		-run "${test_pattern}" -timeout="${INTEGRATION_TEST_TIMEOUT:-8m}"
done <"${work_dir}/selected"
