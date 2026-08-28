package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/catalogpromotion"
)

const (
	databaseURLEnvironment             = "VELA_CATALOG_PROMOTION_DATABASE_URL"
	supplyChainPolicyEnvironment       = "VELA_CATALOG_PROMOTION_SUPPLY_CHAIN_POLICY"
	supplyChainPolicyDigestEnvironment = "VELA_CATALOG_PROMOTION_SUPPLY_CHAIN_POLICY_SHA256"
)

func main() {
	os.Exit(run(
		os.Args[1:],
		os.Getenv(databaseURLEnvironment),
		os.Getenv(supplyChainPolicyEnvironment),
		os.Getenv(supplyChainPolicyDigestEnvironment),
		os.Stdout,
		os.Stderr,
	))
}

func run(
	arguments []string,
	databaseURL,
	supplyChainPolicy,
	supplyChainPolicyDigest string,
	stdout,
	stderr io.Writer,
) int {
	if len(arguments) != 1 {
		_, _ = fmt.Fprintln(stderr, "usage: vela-catalog-promoter <catalog-promotion.json>")
		return 2
	}
	if databaseURL == "" {
		_, _ = fmt.Fprintf(stderr, "%s is required\n", databaseURLEnvironment)
		return 2
	}
	if supplyChainPolicy == "" {
		_, _ = fmt.Fprintf(stderr, "%s is required\n", supplyChainPolicyEnvironment)
		return 2
	}
	if supplyChainPolicyDigest == "" {
		_, _ = fmt.Fprintf(stderr, "%s is required\n", supplyChainPolicyDigestEnvironment)
		return 2
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "open Catalog Promotion database pool: %v\n", err)
		return 1
	}
	defer pool.Close()
	service, err := catalogpromotion.New(ctx, pool, catalogpromotion.SupplyChainPolicySource{
		Path: supplyChainPolicy, Digest: supplyChainPolicyDigest,
	})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "configure Catalog Promotion: %v\n", err)
		return 1
	}
	result, err := service.Apply(ctx, arguments[0])
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "apply Catalog Promotion: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(
		stdout,
		"PROMOTED release=%s manifest=%s protocol=%d receipts=%d\n",
		result.ReleaseDigest,
		result.ManifestDigest,
		result.ProtocolVersion,
		len(result.ReceiptIDs),
	)
	return 0
}
