# fetch_rest Job

Calls REST APIs once a day and writes each response into the inbox.
From there file_router and the ingest job load it like any uploaded
file:

```
fetch_rest job (daily 04:17 Central)  ->  utility.raw.inbox/<inbox_name>
file_router (inbox trigger)           ->  utility.raw.files/<source>/
ingest (raw/files trigger)            ->  bronze and silver tables
```

The daily schedule is paused in free and dev (fetch_pause_status
in **databricks.yml**), so there the job runs only when started by hand.

It works like the fetch_url job (**doc/jobs/fetch_url.md**), with what an API
call needs on top: a method, query parameters, headers, a JSON body,
keys from Databricks secrets, and picking the records out of the
response. It makes one call per settings file; there's no paging.

## Settings

One JSON file per API call in **fetch_rest/settings/**, named
**\<source>.\<name>.json**. Rename a file off .json to disable it.

```
{
    "environments": ["dev", "staging", "prod"],
    "url": "https://api.stlouisfed.org/fred/series/observations",
    "method": "GET",
    "params": { "series_id": "FEDFUNDS", "file_type": "json" },
    "headers": { "Accept": "application/json" },
    "secret_params": {
        "api_key": { "scope": "my-scope", "key": "fred-api-key" }
    },
    "secret_headers": {
        "Authorization": { "scope": "my-scope", "key": "token", "prefix": "Bearer " }
    },
    "records_path": "observations",
    "inbox_name": "fred_fedfunds.json"
}
```

```
environments    where it runs (see doc/environments.md)
url             must start with https://
inbox_name      the file name in the inbox: <source>_<filename>.json,
                where <source> is a folder under raw/files (the
                file_router naming rule). file_router adds the
                timestamp, so leave it out here.
method          optional: GET (default) or POST
params          optional: query string parameters
headers         optional: request headers
body            optional, POST only: sent as the JSON body
secret_params,  optional: like params and headers, but each value
secret_headers  is read from a Databricks secret at run time.
                "prefix" is put in front of the value, e.g.
                "Bearer ". Use these for every key and token, so
                none are in git.
records_path    optional: the top-level key of the response that
                holds the list of records. When set, the file is
                newline-delimited JSON, one record per line, so the
                bronze settings need "multiLine": "false". Without
                it the response is written as it came.
record_key      optional, needs records_path: when each list item
                wraps its record under a key, e.g.
                {"data": [{"data": {...}, "score": 0}, ...]},
                set records_path "data" and record_key "data".
```

task_values checks every file, including ones that don't run in
this environment, so a mistake fails in free first. Every file is
called at the job's one daily time (**resources/fetch_rest.job.yml**).

Examples: **doc/examples/fetch_rest/** (GET with a secret key, POST, and
an enveloped list). The matching bronze example for FRED is
**doc/examples/bronze/fred.fedfunds.json**.

## Tasks

```
task_values   task_values: lists the settings files that run in
              this environment
fetch_loop    fetch, once per file (4 at a time): calls the API
              and writes the response to the inbox as inbox_name
```

Each fetch_iteration prints a FETCH_REST REPORT for its own call:
"Fetched" with the inbox path, size and record count, or "Failed"
with the error. Secret values are replaced by \*\*\* in the error. A
failed call fails that iteration only, and never leaves a partial
file in the inbox.

## Add a Source

1. If the API needs a key, store it as a secret, once per
workspace:

```
databricks secrets create-scope my-scope -p <profile>
databricks secrets put-secret my-scope fred-api-key -p <profile>
```

2. Create the source folder under **raw/files**, if it isn't there:

```
databricks fs mkdir --profile <profile> dbfs:/Volumes/<prefix>utility/raw/files/<source>
```

file_router only routes to folders that exist; anything else
goes to inbox/bad_files.

3. Add the settings file to **fetch_rest/settings/**.

4. Add the bronze (and silver) settings for the table, as in
**doc/procedures/add_table.md**. Without them the file is routed
but nothing loads it.

5. Ship it on a topic branch (**doc/procedures/git_workflow.md**).

## Egress and the Test Source

The free workspace only reaches allowlisted hosts (see "Egress" in
**doc/jobs/fetch_url.md**), so most real APIs can't be called there.

The test source, **fetch_rest/settings/fetchtest.pypi.json**, runs in
free only. It calls pypi's JSON API for the requests package (GET,
with an Accept header and records_path "files") to check that
fetch_rest -> file_router still works. The schedule is paused in
free, so run the job by hand (databricks bundle run -t free
fetch_rest). POST has not been tested against a live API yet.
