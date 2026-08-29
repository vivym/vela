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

test_list=$(mktemp)
trap 'rm -f "${test_list}"' EXIT HUP INT TERM
go test -tags=integration ./internal/integration -list '^Test' >"${test_list}"

test_pattern=$(
	awk -v shard_index="${shard_index}" -v shard_total="${shard_total}" '
		/^Test[[:alnum:]_]+$/ {
			if (test_count % shard_total == shard_index) {
				if (pattern != "") {
					pattern = pattern "|"
				}
				pattern = pattern $0
				shard_count++
			}
			test_count++
		}
		END {
			if (test_count == 0 || shard_count == 0) {
				exit 2
			}
			printf "^(%s)$\n", pattern
		}
	' "${test_list}"
)

echo "running integration shard ${shard_index}/${shard_total}"
go test -tags=integration ./internal/integration/... -count=1 \
	-run "${test_pattern}" -timeout="${INTEGRATION_TEST_TIMEOUT:-8m}"
