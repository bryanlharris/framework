# Recovery

How to undo a bad file, and the lookups that help. Replace
**\<table>** with a full table name, **\<path>** with the file's full
path, and **\<checkpoint>** with the table's checkpointLocation.

Two ways to undo a bad file:

- DELETE: removes only that file's rows. Works for any
file, however old.
- RESTORE: puts the table back to just before the file
was loaded. Only when the bad file was the last thing loaded and
was loaded on its own (step 2 checks). This is what validate_bronze
prints when it finds duplicates.

## Back Out a File (DELETE)

1. Find the file's path. Every bronze row carries it:

```
SELECT source_metadata.file_path, count(*)
FROM <table>
GROUP BY 1
ORDER BY 1;
```

2. Check that silver is caught up: the last ingest run finished
successfully after the bad file was loaded. Step 4 makes silver
skip every bronze version before the DELETE, so anything silver
hadn't read yet would be skipped too. If unsure, let one run finish
first.

3. Delete its rows from bronze. No version number is needed:

```
DELETE FROM <table>
WHERE source_metadata.file_path = '<path>';
```

4. Fix silver. Silver already holds the file's rows (step 2), and the
DELETE stops its stream (it isn't an append). Silver tables keep
file_path as a column:

```
DELETE FROM <silver table>
WHERE file_path = '<path>';
```

For an SCD2 table (history.scd2), check by hand instead: the bad
file may have expired older versions that should be current again.

Then let silver's stream skip the DELETE. In each silver table's
readStream_options, add:

```
"ignoreDeletes": true
"startingVersion": <the DELETE's bronze version + 1>
```

(DESCRIBE HISTORY shows the DELETE's version.) Clear the silver
checkpoint.

5. Remove the bad file from raw/files, so a later checkpoint reset
can never load it again:

```
databricks fs rm --profile <profile> <path>
```

6. Upload the corrected file, if there is one. The router adds a new
timestamp to its name, so Auto Loader reads it as a new file.

7. Run the job. After a successful run, remove startingVersion from
the silver settings.

## Restore to Before a File (RESTORE)

When validate_bronze stops the job on duplicates, it prints these
steps with the exact versions filled in (**doc/jobs/ingest.md**).

1. Find the version that loaded the file (see "Lookups", "Which Table
Version Loaded a File?"). After a validate_bronze stop, it's the
newest version.

2. Check all three, or use DELETE instead:
- Nothing after that version matters: RESTORE drops every later
write.
- That version loaded only this file: Auto Loader can load several
files in one version, and RESTORE drops them all.
- Silver had read every bronze version before it: step 4 makes
silver skip everything up to the reload. A validate_bronze stop
meets this, since silver caught up on the previous run.

```
DESCRIBE HISTORY <table>;
```

3. Restore to the version before it:

```
RESTORE TABLE <table> TO VERSION AS OF <version - 1>;
```

4. Fix silver. If silver already ran on the bad file, delete its rows
as in DELETE, step 4. After a validate_bronze stop it hasn't:
validate_bronze runs before silver, so skip that.

Either way, let silver's stream skip the RESTORE. In each silver
table's readStream_options, add:

```
"ignoreDeletes": true
"startingVersion": <the RESTORE's bronze version + 1>
```

(The RESTORE is one new version; the reload after it is the next.)
Clear the silver checkpoint.

5. Deal with the source files in raw/files. Choose one:
- a. New filename: delete the bad file(s), then upload the file you
want to keep. The router gives it a new timestamped name, which the
bronze checkpoint has never seen, so no checkpoint or modifiedAfter
change is needed.
- b. Reload in place: delete only the unwanted file(s). Then clear
the bronze checkpoint and set modifiedAfter in the bronze
readStream_options to just before the modification time of the
file you keep.

6. Run the job. After a successful run, remove startingVersion from
the silver settings, and modifiedAfter from the bronze settings if
you chose 5b.

## Lookups

### Which File Did a Row Come From?

Every bronze row carries source_metadata.file_path and
source_metadata.file_modification_time, plus ingest_time. Every
silver function calls add_ingest_metadata(), so silver tables also
have file_path and file_modification_time as top-level columns
(source_metadata is kept as well).

```
SELECT source_metadata.file_path, count(*)
FROM <table>
GROUP BY 1
ORDER BY 1;
```

### Which Files Has Auto Loader Processed?

```
SELECT * FROM cloud_files_state('<checkpoint>');
```

### Which Table Version Loaded a File?

If the table has row tracking (delta.enableRowTracking = true), each
row knows the version it was written in. Bronze is append-only, so
this is the version that loaded the file:

```
SELECT source_metadata.file_path,
       min(_metadata.row_commit_version) AS version
FROM <table>
GROUP BY 1
ORDER BY 2;
```

Without row tracking, match ingest_time to the commit timestamps. The
commit that loaded a file is the first one at or after the file's
ingest_time:

```
WITH files AS (
  SELECT source_metadata.file_path, max(ingest_time) AS ingest_time
  FROM <table>
  GROUP BY 1
),
hist AS (
  SELECT version, timestamp
  FROM (DESCRIBE HISTORY <table>)
  WHERE operation IN ('STREAMING UPDATE', 'WRITE', 'MERGE')
)
SELECT f.file_path, min(h.version) AS version
FROM files f
JOIN hist h ON h.timestamp >= f.ingest_time
GROUP BY 1
ORDER BY 2;
```

If (DESCRIBE HISTORY ...) is not accepted as a subquery on your
runtime, run DESCRIBE HISTORY on its own and read the versions off.

### What Happened to a Table? (Operations, Row Counts, Which Job)

```
DESCRIBE HISTORY <table>;
```

Delta keeps this for delta.logRetentionDuration (default 30 days).
Versions you can time travel to are also limited by
delta.deletedFileRetentionDuration (default 7 days) and VACUUM.
