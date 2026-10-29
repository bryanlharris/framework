# file_router Job

Moves each upload from the inbox to its source's folder in
**raw/files**, where the ingest job picks it up. Uploads come from the
scripts in **doc/examples/ps1** or from the fetch jobs:

```
upload or fetch job          ->  utility.raw.inbox/<source>_<name>.<ext>
file_router (inbox trigger)  ->  utility.raw.files/<source>/<name>_<timestamp>.<ext>
ingest (raw/files trigger)   ->  bronze and silver tables
```

The code is **file_router/file_router.py**; the job is
**resources/file_router.job.yml**.

## Trigger and Parameter

The job starts when anything lands in **/Volumes/\<prefix>utility/raw/inbox/**
(file arrival, after 60 seconds with no new changes). Runs don't
overlap: one at a time, and a new trigger waits in the queue.

```
catalog   the utility catalog to route in, e.g. dev_utility; set from
          catalog_prefix by the target in databricks.yml
```

## Naming Rule

The source is the start of the name, before the first underscore
that matches a source. A source is a folder under **raw/files**; make it
before the first upload (**doc/procedures/add_table.md** step 3).

```
fred_fedfunds.csv     ->  raw/files/fred/fedfunds_20261024_104512.csv
edsm_powerPlay.json   ->  raw/files/edsm/powerPlay_20261024_104512.json
```

- The router adds the run's timestamp, so the same name can be
uploaded again without overwriting.
- Longest source first: with sources staging and staging_monthly,
**staging_monthly_x.json** goes to staging_monthly.
- Something must follow the underscore: **fred_.csv** is not routed.

## Folders

A folder in the inbox is routed the same way, by its name, and keeps
its contents:

```
fred_daily/   ->  raw/files/fred/daily/20261024_104512/
```

## Unmatched Uploads

Anything whose name matches no source goes to **raw/inbox/bad_files/**,
with the timestamp added, and is not loaded. Fix the name (or make
the missing source folder) and upload it again.

## Report

Each run prints a report in its output: every upload routed and
where it went, every one moved to bad_files, and the total.
