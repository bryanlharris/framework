# Ideas

Things that might be worth doing someday. Nothing here is planned;
move an idea to **TODO.md** when it's decided to do it. Settled
questions go in **decisions.md**.

## 1. Fetch Jobs (fetch_url, fetch_sftp, fetch_rest)

For all of them:
- Skip unchanged data: send ETag / If-Modified-Since, or compare
a hash with the last fetch, so an unchanged source triggers no
file_router or ingest run.
- Failure alerts: email_notifications in the job YAML.
- Retries with backoff inside one run, for flaky servers.
- Unpack zip or gzip before writing to the inbox.
- A job parameter to run one source only (e.g.
only=fred.fedfunds), for manual reruns.
- Per-source fetch times: a small external script that reads
schedules from the settings files and triggers the job (or one
source) at each time. The job's one daily cron stays the only
built-in schedule.

fetch_sftp:
- Folders where new files build up: fetch only files not fetched
before.

fetch_rest:
- Paging, for APIs that return results a page at a time.
- OAuth client credentials: get a token, then call the API.

New fetch jobs:
- fetch_s3: copy objects from S3, ADLS or GCS into the inbox.
- fetch_email: attachments from an IMAP mailbox.
- fetch_sharepoint: files from SharePoint or OneDrive (Graph API).
- fetch_sql: run a query and write the result as a file. Check
Lakehouse Federation first; it may make this unnecessary.

## 2. Keep Table History Forever

DESCRIBE HISTORY only goes back as far as delta.logRetentionDuration
(30 days by default). If audit or retention needs come up, a
separate repo could hold one opt-in scheduled job (weekly) that
copies DESCRIBE HISTORY of each table into an archive table,
merged on (table_name, version). It finds its own tables (e.g.
information_schema), so it works for any Delta table. Start from
ingest's old **07_transaction_history.py** (commit ed98fce and
earlier). Raising logRetentionDuration is the lighter alternative.

## 3. Split Ingest by Layer, and Number the Notebooks Again

The ingest notebook runs three times (bronze_loop, silver_loop,
silver_dep_loop), which is why numbers couldn't match the run order
and were dropped (commit c811d6a). Split it into one notebook per
loop and the numbers become true, each name matching its task:

```
ingest/
  01_task_values      task_values
  02_bronze           bronze_loop
  03_validate_bronze  validate_bronze_loop
  04_silver           silver_loop
  05_silver_dep       silver_dep_loop
```

- Move the dispatch (read the settings file, call its function_path)
into one lakehouse function. 02, 04 and 05 are then a few lines
each, calling it with their layer, so the logic isn't copied.
- Each refuses a settings file from the wrong layer. Today one
notebook accepts anything, so a file in the wrong list would run
in the wrong loop without complaint.
- Number the fetch folders again too (01_task_values, 02_fetch),
so the repo keeps one convention.

Scope: the lakehouse function and its layer check; git mv and new
notebooks; notebook_path in **resources/\*.job.yml**; names in
**doc/jobs/ingest.md** (and its Task value lists table), **README.md**,
**api.md**, **procedures/recovery.md** and the **doc/jobs/fetch_\*.md** docs. One change on a topic
branch, deployed with the merge.
