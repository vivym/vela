# Freeze external release inputs

This command copies explicitly inventoried Kubernetes ConfigMaps and selected
Secret keys into immutable objects with canonical content revisions. It keeps
the original objects and does not switch running workloads. Secret values stay
in memory and kubectl stdin; receipts contain identities, keys and digests.

```sh
go build -o /tmp/release-external-inputs ./hack/release-external-inputs
/tmp/release-external-inputs source-plan.json > prepare.json
/tmp/release-external-inputs --apply source-plan.json > apply.json
```

The source plan uses `schema_version: 1` and a `resources` array. Each entry
requires `kind`, `namespace`, `name`, `uid`, `resource_version`, and `consumers`.
Secret entries also require sorted `required_keys`. Obtain identities from a
fresh inventory. All source identities are checked again before the first
creation; existing targets must exactly match their expected immutable content.

Names include the first 12 hexadecimal characters of the selected source
content digest. The final `revision` also binds the new object name, so it is
different from the suffix. Use the receipt's full revision for
`vela.ai/release-revision` and external release declarations.

After both preparation and creation pass, verify application/schema compatibility
and prepare a separate workload change to consume the new references. Keep
before/after templates and the failed attempts as distinct evidence. Reverting
references also requires the corresponding application image and database
privilege boundary to be compatible; the MarsLab schema-96 adoption report
records one such coordinated change.

For credential or certificate rotation, create another immutable snapshot and
roll consumers to it. Updating an original mutable Secret does not update a
frozen copy. This command does not create credentials, renew certificates, or
produce a complete all-component release bundle.
