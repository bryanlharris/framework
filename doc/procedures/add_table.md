# Add a New Table to the Pipeline

Names below are for prod and free. In dev and staging every catalog
starts with the prefix (dev_utility, dev_edsm, ...); see
**doc/environments.md**.

1. Create the catalog, once per source. In Databricks, run:

```
CREATE CATALOG IF NOT EXISTS <catalog>;
```

Example:

```
CREATE CATALOG IF NOT EXISTS edsm;
```

2. Add the schemas, once per catalog. The bundle manages them: add a
**\<catalog>_bronze** and **\<catalog>_silver** entry under schemas:
in each active target of **databricks.yml** whose environment the
source runs in (copy an existing one; keep prevent_destroy, except in dev), then
validate and deploy. Optional: a **\<catalog>_gold** entry the same
way.

3. Make the source's folder under **utility.raw.files**, before the
first upload:

```
databricks fs mkdir --profile <profile> dbfs:/Volumes/utility/raw/files/<catalog>
```

4. Create a settings file:

- Place the file in **ingest/settings/bronze/** or **ingest/settings/silver/**
- The file must end in .json to be active; rename the extension to
disable it without deleting it
- Naming convention: **\<catalog>.\<table>.json** (e.g. **fred.fedfunds.json**)
The catalog keeps names unique across sources (fdic.financials vs
another source's financials); the folder gives the schema.
- Every settings file must contain the key "function_path", which
determines which ingest function is called. Pick one from
**doc/api.md** part 1, which also lists the keys each one requires.
- Every settings file must contain the key "environments", listing
where it runs (see **doc/environments.md**)
- Every table name (destination_table, source_table, depends_on)
must start with ${catalog_prefix} (see **doc/environments.md**)
- Easiest start: copy a working file from **ingest/settings/** or
**doc/examples/** and change the names.
- Notes on individual keys (checkpointLocation, badRecordsPath,
modifiedAfter, ...) are in **doc/api.md** part 3.

5. Ship it. The jobs pull settings from git on every run, so a new or
changed settings file does nothing until it is committed and on the
branch or tag the workspace runs. Commit it on a topic branch and
follow **doc/procedures/git_workflow.md**.

6. Upload a file. Name it **\<catalog>_\<name>.\<ext>** (or a folder
**\<catalog>_\<name>**) and put it in the inbox; file_router moves it
to the source's folder and the ingest job loads it. Decompress it
first (see Compressed files below). Optional: add an upload script
for the source, like the ones in **doc/examples/ps1/**, or, if
Databricks can reach the server, a fetch_url, fetch_sftp or
fetch_rest settings file (**doc/jobs/fetch_url.md**,
**doc/jobs/fetch_sftp.md**, **doc/jobs/fetch_rest.md**).

**Volumes.** All raw files and all streaming state live in the
utility catalog, not in per-catalog volumes. These already exist in
every set-up workspace (in a new one, the bundle creates them on the
first deploy; see **doc/procedures/bundle_deploy.md** "First Deploy, New Workspace"):

```
utility.raw.inbox      uploads land here; file_router moves them
utility.raw.files      one folder per source (source = catalog);
                       any new file here triggers the ingest job
utility.bronze.state   bronze checkpoints, schemas, bad records
utility.silver.state   silver checkpoints
```

file_router moves uploads to these paths; names that don't start
with an existing source folder go to inbox/bad_files:

```
/Volumes/utility/raw/files/<catalog>/<name>_<timestamp>.<ext>
/Volumes/utility/raw/files/<catalog>/<name>/<timestamp>/   (folders)
```

The paths referenced in settings files look like:

```
/Volumes/${catalog_prefix}utility/raw/files/<catalog>/
/Volumes/${catalog_prefix}utility/bronze/state/${catalog_prefix}<catalog>.bronze.<table>/_checkpoints/
/Volumes/${catalog_prefix}utility/bronze/state/${catalog_prefix}<catalog>.bronze.<table>/_schema/
/Volumes/${catalog_prefix}utility/bronze/state/${catalog_prefix}<catalog>.bronze.<table>/_badRecords/
/Volumes/${catalog_prefix}utility/silver/state/${catalog_prefix}<catalog>.silver.<table>/_checkpoints/
```

**Compressed files.** No compressed files of any kind (.gz, .zip,
...) anywhere in the pipeline; **edsm.ps1**, for example, unzips each
**.json.gz** before upload.

**Timestamps in file names.** Bronze ingestion derives a timestamp
from the file path. Every file in **utility.raw.files** must have one
in the format:

```
YYYYMMdd_HHmmss
```

Files uploaded through the inbox get it automatically: file_router
adds it when it moves them (**fred_fedfunds.csv** becomes
**fred/fedfunds_20261022_184628.csv**; folders go under
**\<name>/\<timestamp>/**). Nothing to do for those. The rest is only
for files that land in **utility.raw.files** without going through
the router.

The default regex that extracts the timestamp is:

```
(\d{8}_\d{6})
```

If your files use a different pattern you can override it in the
settings file under derived.file_path_datetime_regex, but the
default pattern is strongly preferred for consistency.

If the captured value is not in YYYYMMdd_HHmmss form, also set
derived.file_path_datetime_format to a Spark datetime pattern
that matches it. Use this for sources whose file names you don't
control. Example for Amazon Data Firehose, whose object names end
in **\<stream>-\<version>-YYYY-MM-DD-HH-MM-SS-\<random>[.ext]**:

```
"derived": {
    "file_path_datetime_regex": "(\\d{4}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2})-[^/]*$",
    "file_path_datetime_format": "yyyy-MM-dd-HH-mm-ss"
}
```

Firehose timestamps are in UTC. The extracted value becomes the
derived_ingest_time column in bronze, which silver uses to track when
data was originally produced rather than when it was ingested.

**Disable a table without deleting it.** Rename the settings file so
it no longer ends in .json, e.g. **edsm.powerPlay.json** ->
**edsm.powerPlay.json.disabled**. The ingest job ignores any file
that does not end in .json.
