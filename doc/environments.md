# Environments

Each workspace's ingest job sets two job parameters.

Environments:

```
environment   catalog_prefix   used for
-----------   --------------   ----------------------
free          (empty)          sandbox free workspace
test          test_            test copy, in the free workspace
dev           dev_             dev workspace
staging       staging_         staging workspace
prod          (empty)          prod workspace
```

prefix = what names start with; environment = which tables run.
prod and free share an empty prefix, so only environment tells them
apart.

Datasets per target:

```
dataset   targets
-------   ---------------
edsm      free, test, dev
fdic      free, test
fred      free, test
treasury  free
```

staging and prod have no datasets yet: only the jobs and the utility
schemas and volumes. A dataset needs both its schemas in the target
(**databricks.yml**) and the target in its settings' environments list.

The bundle sets both per target (**databricks.yml**); see
**doc/procedures/bundle_deploy.md**.

## catalog_prefix

Replaces ${catalog_prefix} anywhere in a settings file before it is
parsed. Includes the underscore, so **/Volumes/${catalog_prefix}utility/**
becomes **/Volumes/dev_utility/** in dev and **/Volumes/utility/** in prod.
Any other ${...} in a settings file is an error.

Every table name starts with the token, in destination_table,
source_table and depends_on: "**${catalog_prefix}fred.bronze.fedfunds**"
becomes **dev_fred.bronze.fedfunds** in dev and **fred.bronze.fedfunds** in
prod. This is required: task_values fails on any table name without
it, so dev, staging and prod can never write to the same catalog.
A dataset kept out of some targets still uses the token (dev_edsm in
dev) and limits itself with its environments list.

## environment

free, test, dev, staging or prod. Every settings file must list where
it runs; there is no default:

```
"environments": ["free", "dev", "staging", "prod"]
"environments": ["free", "dev"]
```

A missing or empty list, or an unknown name, fails the task_values
task. Files not listed for the job's environment are skipped and
printed by task_values.
