# Compare expected top-level tests with Go's verbose terminal events. Nested
# subtests have indented result lines and are not counted as additional tests.
NR == FNR {
  if ($0 in expected) {
    print "duplicate discovered test: " $0 > "/dev/stderr"
    invalid = 1
  }
  expected[$0] = 1
  ordered[++total] = $0
  next
}
/^--- (PASS|SKIP|FAIL): / {
  name = $3
  if (!(name in expected)) {
    print "unexpected test result: " name > "/dev/stderr"
    invalid = 1
  }
  if (name in outcome) {
    print "duplicate test result: " name > "/dev/stderr"
    invalid = 1
  }
  action = $2
  sub(/:$/, "", action)
  outcome[name] = action
}
END {
  print "test\tresult" > result_file
  for (i = 1; i <= total; i++) {
    name = ordered[i]
    action = (name in outcome) ? outcome[name] : "MISSING"
    count[action]++
    print name "\t" action > result_file
  }
  close(result_file)
  printf "PASS=%d SKIP=%d FAIL=%d MISSING=%d\n", count["PASS"], count["SKIP"], count["FAIL"], count["MISSING"]
  if (total == 0 || invalid || count["FAIL"] || count["MISSING"]) exit 1
}
