package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunRequiresPlanAndDatabaseURL(t *testing.T) {
	for _, test := range []struct {
		name     string
		args     []string
		database string
		policy   string
		digest   string
		code     int
		message  string
	}{
		{name: "plan", code: 2, message: "usage:"},
		{
			name: "database URL", args: []string{"promotion.json"}, code: 2,
			message: "VELA_CATALOG_PROMOTION_DATABASE_URL is required",
		},
		{
			name: "supply-chain policy", args: []string{"promotion.json"}, database: "configured", code: 2,
			message: "VELA_CATALOG_PROMOTION_SUPPLY_CHAIN_POLICY is required",
		},
		{
			name: "supply-chain policy digest", args: []string{"promotion.json"}, database: "configured",
			policy: "/policy.json", code: 2,
			message: "VELA_CATALOG_PROMOTION_SUPPLY_CHAIN_POLICY_SHA256 is required",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(test.args, test.database, test.policy, test.digest, &stdout, &stderr)
			if code != test.code || stdout.Len() != 0 ||
				!strings.Contains(stderr.String(), test.message) {
				t.Fatalf(
					"run = code %d stdout %q stderr %q",
					code, stdout.String(), stderr.String(),
				)
			}
		})
	}
}
