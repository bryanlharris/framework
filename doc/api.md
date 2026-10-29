# Lakehouse API

Part 1 lists the functions a settings file can call (its function_path)
and the keys each one needs. Part 2 lists the helpers those functions
use internally. Part 3 has notes on individual settings keys. Each
function's docstring has the details and an example.

```
Every settings file, whatever its function_path, also needs:
  function_path   one of the values in part 1
  environments    where it runs (see doc/environments.md)
and every table name in it must start with ${catalog_prefix}.
```

## Part 1: function_path Values

### Bronze

**lakehouse.bronze.ingest.local.from_file**

Auto Loader stream over a folder, normally
**/Volumes/${catalog_prefix}utility/raw/files/\<catalog>/**. Adds
ingest_time, source_metadata and derived_ingest_time. Writes only to
a bronze schema.

```
+-----------------------------------+----------+------+
| Key                               | Needed   | Note |
+-----------------------------------+----------+------+
| source_type                       | required |      |
+-----------------------------------+----------+------+
| destination_table                 | required |      |
+-----------------------------------+----------+------+
| readStream_path                   | required |      |
+-----------------------------------+----------+------+
| readStream_options                | required |      |
+-----------------------------------+----------+------+
| writeStream_format                | required |      |
+-----------------------------------+----------+------+
| writeStream_options               | required |      |
+-----------------------------------+----------+------+
| writeStream_outputMode            | required |      |
+-----------------------------------+----------+------+
| trigger_type                      | required |      |
+-----------------------------------+----------+------+
| derived.file_path_datetime_regex  | optional |      |
+-----------------------------------+----------+------+
| derived.file_path_datetime_format | optional |      |
+-----------------------------------+----------+------+
| validate_bronze                   | optional |      |
+-----------------------------------+----------+------+
| dedupe_within_file                | optional |      |
+-----------------------------------+----------+------+
```

**lakehouse.bronze.ingest.local.from_pdf**

Batch: reads the newest PDF in a folder, parses it with
ai_parse_document and appends one row.

```
+-------------------+----------+------------------+
| Key               | Needed   | Note             |
+-------------------+----------+------------------+
| destination_table | required |                  |
+-------------------+----------+------------------+
| input_path        | required |                  |
+-------------------+----------+------------------+
| date_pattern      | required |                  |
+-------------------+----------+------------------+
| write_options     | required |                  |
+-------------------+----------+------------------+
| write_mode        | optional | default "append" |
+-------------------+----------+------------------+
```

To load data from a URL, SFTP server or REST API, fetch it into the
inbox with the fetch_url, fetch_sftp or fetch_rest job
(**doc/jobs/fetch_url.md**, **doc/jobs/fetch_sftp.md**, **doc/jobs/fetch_rest.md**) or an
upload script (**doc/examples/ps1**), and load it with from_file.

### Silver

All silver functions stream from source_table and write with
availableNow, so each run processes what is new and stops.

**lakehouse.silver.ingest.from_table.upsert.without_delete**

Upsert by primary key. Rows gone from the source stay in silver
with their last state: the table remembers every row it has seen.

```
+---------------------+----------+----------------------------+
| Key                 | Needed   | Note                       |
+---------------------+----------+----------------------------+
| source_table        | required |                            |
+---------------------+----------+----------------------------+
| destination_table   | required |                            |
+---------------------+----------+----------------------------+
| readStream_options  | required |                            |
+---------------------+----------+----------------------------+
| writeStream_options | required |                            |
+---------------------+----------+----------------------------+
| column_map          | required |                            |
+---------------------+----------+----------------------------+
| data_type_map       | required |                            |
+---------------------+----------+----------------------------+
| pk.name             | required |                            |
+---------------------+----------+----------------------------+
| pk.columns          | required |                            |
+---------------------+----------+----------------------------+
| on_duplicate_pk     | optional | "raise" or "dedupe_latest" |
+---------------------+----------+----------------------------+
| json_column_map     | optional |                            |
+---------------------+----------+----------------------------+
| regex_extract       | optional |                            |
+---------------------+----------+----------------------------+
```

**lakehouse.silver.ingest.from_table.upsert.with_delete**

Like without_delete, but rows gone from the source are deleted:
the table mirrors the source. Delta API merge.

```
+---------------------+----------+------+
| Key                 | Needed   | Note |
+---------------------+----------+------+
| source_table        | required |      |
+---------------------+----------+------+
| destination_table   | required |      |
+---------------------+----------+------+
| readStream_options  | required |      |
+---------------------+----------+------+
| writeStream_options | required |      |
+---------------------+----------+------+
| column_map          | required |      |
+---------------------+----------+------+
| data_type_map       | required |      |
+---------------------+----------+------+
| pk.name             | required |      |
+---------------------+----------+------+
| pk.columns          | required |      |
+---------------------+----------+------+
| on_duplicate_pk     | optional |      |
+---------------------+----------+------+
| json_column_map     | optional |      |
+---------------------+----------+------+
```

**lakehouse.silver.ingest.from_table.merge.with_delete**

Same result as upsert.with_delete, done with a SQL MERGE.

```
+---------------------+----------+------+
| Key                 | Needed   | Note |
+---------------------+----------+------+
| source_table        | required |      |
+---------------------+----------+------+
| destination_table   | required |      |
+---------------------+----------+------+
| readStream_options  | required |      |
+---------------------+----------+------+
| writeStream_options | required |      |
+---------------------+----------+------+
| column_map          | required |      |
+---------------------+----------+------+
| data_type_map       | required |      |
+---------------------+----------+------+
| pk.name             | required |      |
+---------------------+----------+------+
| pk.columns          | required |      |
+---------------------+----------+------+
```

**lakehouse.silver.ingest.from_table.history.scd2**

SCD2 history: a changed row expires the current version and adds a
new one (valid_from, valid_to, current_flag).

```
+---------------------+----------+------------------------------+
| Key                 | Needed   | Note                         |
+---------------------+----------+------------------------------+
| source_table        | required |                              |
+---------------------+----------+------------------------------+
| destination_table   | required |                              |
+---------------------+----------+------------------------------+
| writeStream_options | required |                              |
+---------------------+----------+------------------------------+
| business_key        | required |                              |
+---------------------+----------+------------------------------+
| change_key          | required | columns whose change makes a |
|                     |          | new version; a change to or  |
|                     |          | from NULL counts             |
+---------------------+----------+------------------------------+
| ingest_time_column  | required |                              |
+---------------------+----------+------------------------------+
| readStream_options  | optional |                              |
+---------------------+----------+------------------------------+
| column_map          | optional |                              |
+---------------------+----------+------------------------------+
| data_type_map       | optional |                              |
+---------------------+----------+------------------------------+
| use_row_hash        | optional |                              |
+---------------------+----------+------------------------------+
| row_hash_col        | optional |                              |
+---------------------+----------+------------------------------+
```

**lakehouse.silver.custom.systems_with_power**

One-off for edsm: joins the two tables in depends_on and overwrites
destination_table.

```
+-------------------+----------+----------------------------+
| Key               | Needed   | Note                       |
+-------------------+----------+----------------------------+
| destination_table | required |                            |
+-------------------+----------+----------------------------+
| depends_on        | required | systems table, power table |
+-------------------+----------+----------------------------+
```

Silver tables that read from another silver table (source_table or
depends_on names a silver table) run in silver_dep_loop, after the
others, in dependency order.

## Part 2: Helpers

Used inside the functions above; settings files never name them.

```
+------------------------------+------------------------------------------+
| Function                     | Purpose                                  |
+------------------------------+------------------------------------------+
| lakehouse.core.config                                                   |
+------------------------------+------------------------------------------+
| scan_settings_folder()       | settings files for a layer and           |
|                              | environment                              |
+------------------------------+------------------------------------------+
| extract_destination_tables() | destination tables for a layer and       |
|                              | environment                              |
+------------------------------+------------------------------------------+
| skipped_settings()           | settings files not run in this           |
|                              | environment                              |
+------------------------------+------------------------------------------+
| lakehouse.core.utils                                                    |
+------------------------------+------------------------------------------+
| read_json_and_decode()       | read a settings file, apply              |
|                              | ${catalog_prefix}                        |
+------------------------------+------------------------------------------+
| get_table_schema()           | a table's schema, optionally minus some  |
|                              | columns                                  |
+------------------------------+------------------------------------------+
| ensure_table_exists()        | create a table from a schema string if   |
|                              | missing                                  |
+------------------------------+------------------------------------------+
| create_table_if_not_exists() | create an empty table shaped like a      |
|                              | DataFrame                                |
+------------------------------+------------------------------------------+
| lakehouse.core.transform.metadata                                       |
+------------------------------+------------------------------------------+
| add_timestamp_column()       | add a current_timestamp() column         |
+------------------------------+------------------------------------------+
| lakehouse.bronze.transform.file                                         |
+------------------------------+------------------------------------------+
| add_source_metadata()        | add Spark's _metadata as a named column  |
+------------------------------+------------------------------------------+
| lakehouse.bronze.transform.derived                                      |
+------------------------------+------------------------------------------+
| add_ingest_time_from_path()  | timestamp parsed from the file path by   |
|                              | regex                                    |
+------------------------------+------------------------------------------+
| lakehouse.bronze.utils                                                  |
+------------------------------+------------------------------------------+
| get_latest_file_path()       | newest file in a folder by date in its   |
|                              | name                                     |
+------------------------------+------------------------------------------+
| lakehouse.silver.transform.columns                                      |
+------------------------------+------------------------------------------+
| rename_columns()             | apply column_map                         |
+------------------------------+------------------------------------------+
| cast_data_types()            | apply data_type_map                      |
+------------------------------+------------------------------------------+
| parse_json_columns()         | apply json_column_map                    |
+------------------------------+------------------------------------------+
| regex_extract()              | add columns from regex capture groups    |
+------------------------------+------------------------------------------+
| lakehouse.silver.transform.metadata                                     |
+------------------------------+------------------------------------------+
| make_null_safe()             | stable null handling for hashing         |
+------------------------------+------------------------------------------+
| add_row_hash()               | SHA-256 over chosen columns              |
+------------------------------+------------------------------------------+
| add_ingest_metadata()        | lift file_path and                       |
|                              | file_modification_time out of            |
|                              | source_metadata, add ingest_time         |
+------------------------------+------------------------------------------+
| add_scd2_columns()           | add the SCD2 tracking columns            |
+------------------------------+------------------------------------------+
| lakehouse.silver.merge (each returns a foreachBatch function)           |
+------------------------------+------------------------------------------+
| upsertByPK()                 | insert + update, no deletes              |
+------------------------------+------------------------------------------+
| fullSyncMerge()              | insert + update + delete, Delta API      |
+------------------------------+------------------------------------------+
| fullSyncMergeSQL()           | insert + update + delete, SQL MERGE      |
+------------------------------+------------------------------------------+
| scd2UpsertByBusinessKey()    | SCD2 expire + insert                     |
+------------------------------+------------------------------------------+
```

## Part 3: Notes on Settings Keys

**checkpointLocation**

Put under writeStream_options.
Convention: **/Volumes/${catalog_prefix}utility/\<schema>/state/${catalog_prefix}\<catalog>.\<schema>.\<table>/_checkpoints/**
The prefix appears twice: once for the utility catalog, once in the
folder, which is the table's full name (**dev_edsm.bronze.powerPlay** in
dev). The same goes for schemaLocation and badRecordsPath.

**schemaLocation** (bronze Auto Loader only)

Put under readStream_options.
Convention: **/Volumes/${catalog_prefix}utility/bronze/state/${catalog_prefix}\<catalog>.bronze.\<table>/_schema/**

**badRecordsPath** (bronze only)

Put under readStream_options.
Do not also set "mode" - badRecordsPath implies PERMISSIVE mode.
Convention: **/Volumes/${catalog_prefix}utility/bronze/state/${catalog_prefix}\<catalog>.bronze.\<table>/_badRecords/**

**destination_table**

Must be in the form **${catalog_prefix}\<catalog>.\<schema>.\<table>**.
Bronze functions enforce that the schema is "bronze".

**modifiedAfter** (recovery option, bronze readStream_options)

Skip files modified at or before this ISO 8601 timestamp.
Remove after a successful recovery run.

**validate_bronze** (bronze only, top-level key)

Default: true. Set to false to skip the duplicate row check in
validate_bronze for this table.
Use for append-only sources that deliver at least once (e.g.
Firehose), where duplicates in bronze are expected and are
removed by the silver merge.
See **doc/jobs/ingest.md**.

**dedupe_within_file** (bronze from_file only, top-level key)

Default: false. Set to true to drop rows repeated inside one file
before they reach bronze; rows repeated across files are kept. Use
for sources whose own files repeat rows by mistake (EDSM's
powerPlay dump). Leave it off where identical rows are real events,
where bronze must match the file row for row, for big files (it
shuffles each batch), and for tables with map or variant columns,
which dropDuplicates can't compare.

```
regex_extract  (silver upsert.without_delete only)
  A list of entries, each adding columns from the capture groups of a
  regex. Runs after column_map and before data_type_map.
    source   column to match; may be nested, e.g. source_metadata.file_path
    pattern  regex with one capture group per name in columns
    columns  new column names, filled from groups 1, 2, ... in order
    types    optional; maps a new column to a type, e.g. "timestamp"
  Rows that don't match get null in every column of that entry and
  are kept. Use "types" here rather than data_type_map for these
  columns: data_type_map fails the batch on any null, "types" does
  not. Make the pattern strict enough that a match always converts.
  Example (RDS Postgres logs from Firehose, bronze column value):
    "column_map": { "value": "line" },
    "regex_extract": [
      { "source": "source_metadata.file_path",
        "pattern": "rds/postgresql/([^/]+)/data/",
        "columns": ["instance"] },
      { "source": "line",
        "pattern": "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}) \\w+:(.*?):([^:@]*)@([^:]*):\\[(\\d+)\\]:([A-Z0-9]+):\\s+(.*)$",
        "columns": ["log_time", "remote", "db_user", "db_name", "pid", "severity", "message"],
        "types": { "log_time": "timestamp", "pid": "int" } }
    ],
    "pk": { "name": "primary_key", "columns": ["instance", "line"] },
    "on_duplicate_pk": "dedupe_latest"
```
