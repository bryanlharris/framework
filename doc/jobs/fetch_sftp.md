# fetch_sftp Job

Downloads one file per settings file from an SFTP server into the
inbox once a day. From there file_router and the ingest job load it
like any uploaded file:

```
fetch_sftp job (daily 04:17 Central)  ->  utility.raw.inbox/<inbox_name>
file_router (inbox trigger)           ->  utility.raw.files/<source>/
ingest (raw/files trigger)            ->  bronze and silver tables
```

The daily schedule is paused in free, test and dev (fetch_pause_status
in **databricks.yml**), so there the job runs only when started by hand.

It works like the fetch_url job (**doc/jobs/fetch_url.md**), for a file on an
SFTP server instead of a URL.

For now each settings file names one remote file that the server
overwrites daily. The job fetches it every day and doesn't track what
it has already fetched, so it isn't suited to a folder where new files
build up.

## Settings

One JSON file per remote file in **fetch_sftp/settings/**, named
**\<source>.\<name>.json**. Rename a file off .json to disable it.

```
{
    "environments": ["dev", "staging", "prod"],
    "host": "sftp.example.com",
    "port": 22,
    "host_key": "ssh-ed25519 AAAA...",
    "username": "svc_account",
    "secret_scope": "my-scope",
    "secret_key": "sftp-private-key",
    "remote_file": "/outbound/export.csv",
    "inbox_name": "partner_export.csv"
}
```

```
environments   where it runs (see doc/environments.md)
host, port     the server; port is optional (default 22)
host_key       the server's public key, "<type> <base64>". The
               connection fails if the server presents another key,
               so a spoofed server never gets the login. Get it with
                 ssh-keyscan -t ed25519 sftp.example.com
               and check it with the server's owner.
username       the SFTP login
secret_scope,  the Databricks secret holding the private key (RSA,
secret_key     Ed25519 or ECDSA, PEM or OpenSSH format)
remote_file    the absolute path of the file on the server
inbox_name     the file name in the inbox: <source>_<filename>.<ext>,
               where <source> is a folder under raw/files (the
               file_router naming rule). file_router adds the
               timestamp, so leave it out here.
```

task_values checks every file, including ones that don't run in
this environment, so a mistake fails in free first. Every file is
fetched at the job's one daily time (**resources/fetch_sftp.job.yml**).

Examples: **doc/examples/fetch_sftp/**. **fetch_sftp/settings/** holds only a
disabled copy of the example, so the job currently fetches nothing.

## Tasks

```
task_values   task_values: lists the settings files that run in
              this environment
fetch_loop    fetch, once per file (4 at a time): connects,
              downloads remote_file to local disk, then copies it
              to the inbox as inbox_name
```

Each fetch_iteration prints a FETCH_SFTP REPORT for its own file:
"Fetched" with the inbox path and size, or "Failed" with the error.
A failed fetch fails that iteration only, and never leaves a partial
file in the inbox. The job installs paramiko from pypi (see the job
YAML).

## Add a Source

1. Store the private key as a secret, once per workspace:

```
databricks secrets create-scope my-scope -p <profile>
databricks secrets put-secret my-scope sftp-private-key -p <profile> < id_ed25519
```

2. Create the source folder under **raw/files**, if it isn't there:

```
databricks fs mkdir --profile <profile> dbfs:/Volumes/<prefix>utility/raw/files/<source>
```

file_router only routes to folders that exist; anything else
goes to inbox/bad_files.

3. Add the settings file to **fetch_sftp/settings/**.

4. Add the bronze (and silver) settings for the table, as in
**doc/procedures/add_table.md**.

5. Ship it on a topic branch (**doc/procedures/git_workflow.md**).

## Not Tested Against a Server Yet

The free workspace only reaches allowlisted hosts (see "Egress" in
**doc/jobs/fetch_url.md**), so no SFTP server could be reached there. The job
was tested only with no settings: it deploys, installs paramiko and
runs green with nothing to fetch. Test the download with the first
real source, in dev.
