# fetch_url Job

Downloads files from URLs into the inbox once a day. From there
file_router and the ingest job load them like any uploaded file:

```
fetch_url job (daily 04:17 Central)  ->  utility.raw.inbox/<inbox_name>
file_router (inbox trigger)          ->  utility.raw.files/<source>/
ingest (raw/files trigger)           ->  bronze and silver tables
```

The daily schedule is paused in free, test and dev (fetch_pause_status
in **databricks.yml**), so there the job runs only when started by hand.

It does the same job as an upload script in **doc/examples/ps1**, but runs
in Databricks, so it only works for URLs Databricks can reach (see
"Egress" below).

## Settings

One JSON file per URL in **fetch_url/settings/**, named
**\<source>.\<name>.json**. Rename a file off .json to disable it.

```
{
    "environments": ["free", "dev", "staging", "prod"],
    "url": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=FEDFUNDS",
    "inbox_name": "fred_fedfunds.csv"
}
```

```
environments   where it runs (see doc/environments.md)
url            must start with https://
inbox_name     the file name in the inbox: <source>_<filename>.<ext>,
               where <source> is a folder under raw/files (the
               file_router naming rule). file_router adds the
               timestamp, so leave it out here.
gunzip         optional, true or false (default false). true unzips
               a .gz download on the way in, as the upload scripts
               do; inbox_name is then the unzipped name, e.g.
               edsm_powerPlay.json for .../powerPlay.json.gz.
```

The download goes to local disk first, then into the inbox in one
copy, so file_router never sees a half-written file. It sends the
User-Agent fetch_url/1.0; www.edsm.net answers 403 to the requests
default.
task_values checks every file, including ones that don't run in
this environment, so a mistake fails in free first. There is no
schedule per file: every URL is fetched at the job's one daily time
(**resources/fetch_url.job.yml**).

Examples: **doc/examples/fetch_url/**.

## Tasks

```
task_values   task_values: lists the settings files that run in
              this environment
fetch_loop    fetch, once per file (4 at a time): downloads the
              URL (unzipping it with gunzip) and writes it to the
              inbox as inbox_name
```

Each fetch_iteration prints a FETCH_URL REPORT for its own URL:
"Fetched" with the inbox path and size, or "Failed" with the error.
A failed fetch fails that iteration only; the others still run.

## Add a Source

1. Create the source folder under **raw/files**, if it isn't there:

```
databricks fs mkdir --profile <profile> dbfs:/Volumes/<prefix>utility/raw/files/<source>
```

file_router only routes to folders that exist; anything else
goes to inbox/bad_files.

2. Add the settings file to **fetch_url/settings/**.

3. Add the bronze (and silver) settings for the table, as in
**doc/procedures/add_table.md**. Without them the file is routed
but nothing loads it.

4. Ship it on a topic branch (**doc/procedures/git_workflow.md**).

## Egress

The free workspace's serverless compute only reaches allowlisted
hosts. github.com, **raw.githubusercontent.com** and pypi.org work;
**api.fdic.gov**, **www.edsm.net** and example.com don't resolve, and
**fred.stlouisfed.org** times out. Use an upload script for those.

The test source, **fetch_url/settings/fetchtest.license.json**, runs in
free only. It fetches a fixed public file (the cpython **v3.12.0**
LICENSE) to check that fetch_url -> file_router -> ingest still
works. The schedule is paused in free, so run the job by hand
(databricks bundle run -t free fetch_url). This repo is private, so
it can't fetch its own files without a token.
