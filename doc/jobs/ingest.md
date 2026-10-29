# Ingest Job

Loads the files in **raw/files** into bronze and silver tables. One
JSON settings file per table, in **ingest/settings/**, says how; the
code is the same for every table:

```
file_router (inbox trigger)  ->  utility.raw.files/<source>/
ingest (raw/files trigger)   ->  bronze and silver tables
```

## Trigger and Parameters

The job starts when files land anywhere under
**/Volumes/\<prefix>utility/raw/files/** (file arrival, after 60 seconds
with no new changes). Runs don't overlap: one at a time, and a new
trigger waits in the queue. Each run goes through every settings
file that runs in this environment; Auto Loader skips files a table
has already loaded.

```
catalog_prefix   put in front of catalog names (dev_, staging_, empty)
environment      selects settings files by their "environments" list
```

Both come from the target in **databricks.yml**; see **doc/environments.md**.

## Tasks

The notebooks in **ingest/** are the job's tasks. They run in this
order:

```
task_values
ingest                     (bronze_loop)
validate_bronze
ingest                     (silver_loop, then silver_dep_loop)
```

The notebooks have no numbers: ingest runs both before and after
validate_bronze, so no numbering could match the run order. The
order is set in **resources/ingest.job.yml**. (Removed notebooks: a
landing-zone check before bronze, now covered by the silver upsert's
on_duplicate_pk; and file and table history, which **doc/procedures/recovery.md**
shows how to get now.)

### task_values

Scans **ingest/settings/bronze/** and **ingest/settings/silver/** for active
.json files that run in this environment and publishes four lists as
job task values. Each loop task in the job runs once per entry of one
list: 4 at a time, except silver_dep_loop, which runs one at a time
so dependency order holds.

Task value lists:

```
+---------------+-----------------------------+----------------------+--------------------+
| List          | One entry is                | Loop task            | Notebook           |
+---------------+-----------------------------+----------------------+--------------------+
| bronze        | a bronze settings file      | bronze_loop          | ingest             |
+---------------+-----------------------------+----------------------+--------------------+
| bronze_tables | a bronze table name and     | validate_bronze_loop | validate_bronze    |
|               | its settings file           |                      |                    |
+---------------+-----------------------------+----------------------+--------------------+
| silver        | a silver settings file      | silver_loop          | ingest             |
|               | whose source is not silver  |                      |                    |
+---------------+-----------------------------+----------------------+--------------------+
| silver_dep    | a silver settings file that | silver_dep_loop      | ingest             |
|               | reads another silver table, |                      |                    |
|               | in dependency order         |                      |                    |
+---------------+-----------------------------+----------------------+--------------------+
```

Example bronze_tables entry (table name already has the prefix applied):

```
{"full_table_name": "fred.bronze.fedfunds",
 "settings_file":   "settings/bronze/fred.fedfunds.json",
 "catalog_prefix":  ""}
```

settings_file is relative to **ingest/**, the folder the notebooks run in.

Every entry carries catalog_prefix so the notebook can resolve
${catalog_prefix} in its settings file.

### ingest

Runs once per settings file, for bronze, silver and silver_dep. Reads
the settings file, imports the function named in function_path, and
calls it with (spark, settings). All ingest logic lives in the
lakehouse package; see **doc/api.md**.

### validate_bronze

Runs once per bronze table, after bronze ingest and before silver.
Compares count() with distinct().count() over the whole table. If
there are duplicate rows, it fails and lists the source files that
contain them, along with recovery steps (RESTORE the table, fix the
source files, restart silver from the right version).

Why: for snapshot sources, duplicate rows in bronze mean a file was
ingested twice or a bad export got through, and silver should not
run on top of that.

Opt out per table with "validate_bronze": false in the bronze
settings file (see **doc/api.md** part 3). The task still runs for that
table but exits straight away as a success. Do this for append-only
sources such as Firehose, which deliver at least once, so duplicates
in bronze are expected and are removed by the silver merge. For a
source whose files repeat rows by mistake, use "dedupe_within_file":
true instead, so they never reach bronze. Note that
the check scans the whole table, so it gets slower as the table grows.
