# Framework Docs: Start Here

The framework loads files into bronze and silver Delta tables in
Databricks. Each table is described by one JSON settings file;
the code is the same for every table.

## How Data Flows

```
upload script (doc/examples/ps1), or the fetch_url / fetch_sftp / fetch_rest jobs (daily)
      |
      v
utility.raw.inbox  --file_router job-->  utility.raw.files/<source>/
                                                |
                                   (new file triggers the ingest job)
                                                v
task_values -> bronze_loop -> validate_bronze_loop -> silver_loop
                                                   -> silver_dep_loop
```

All the jobs pull their code from GitHub on every run; the bundle
(**databricks.yml**) defines the jobs in each workspace.

## Repo Layout

```
databricks.yml    the bundle: one target per workspace
resources/        one .job.yml per job
lakehouse/        shared Python library (the ingest functions)
ingest/           the ingest job: its notebooks, and settings/
                  with one JSON per bronze or silver table
file_router/      the file_router job
fetch_url/        the fetch_url job: its notebooks, and settings/
                  with one JSON per URL to fetch
fetch_sftp/       the fetch_sftp job: its notebooks, and settings/
                  with one JSON per SFTP file to fetch
fetch_rest/       the fetch_rest job: its notebooks, and settings/
                  with one JSON per API call
doc/              these docs
```

## Which File Answers Which Question

Questions:

```
+-----------------------------------------------+-------------------------------+
| Question                                      | File                          |
+-----------------------------------------------+-------------------------------+
| How do I add a new source or table?           | procedures/add_table.md       |
+-----------------------------------------------+-------------------------------+
| How do I download a file on a schedule?       | jobs/fetch_url.md             |
+-----------------------------------------------+-------------------------------+
| How do I fetch a file from an SFTP server?    | jobs/fetch_sftp.md            |
+-----------------------------------------------+-------------------------------+
| How do I load data from a REST API?           | jobs/fetch_rest.md            |
+-----------------------------------------------+-------------------------------+
| How does file_router route an upload?         | jobs/file_router.md           |
+-----------------------------------------------+-------------------------------+
| Which function_path can I use, and its keys?  | api.md                        |
+-----------------------------------------------+-------------------------------+
| Why is it dev_fred here and fred there?       | environments.md               |
+-----------------------------------------------+-------------------------------+
| Why didn't my table run in this workspace?    | environments.md               |
+-----------------------------------------------+-------------------------------+
| How does a change get from a branch to prod?  | procedures/git_workflow.md    |
+-----------------------------------------------+-------------------------------+
| How do I release to dev?                      | procedures/release_dev.md     |
+-----------------------------------------------+-------------------------------+
| How do I release to staging?                  | procedures/release_staging.md |
+-----------------------------------------------+-------------------------------+
| How do I release to prod?                     | procedures/release_prod.md    |
+-----------------------------------------------+-------------------------------+
| How do I deploy or change the jobs?           | procedures/bundle_deploy.md   |
+-----------------------------------------------+-------------------------------+
| Who deleted a schema or volume?               | procedures/bundle_deploy.md   |
+-----------------------------------------------+-------------------------------+
| What does each ingest task / notebook do?     | jobs/ingest.md                |
+-----------------------------------------------+-------------------------------+
| Which file did these rows come from?          | procedures/recovery.md        |
+-----------------------------------------------+-------------------------------+
| How do I undo a bad file?                     | procedures/recovery.md        |
+-----------------------------------------------+-------------------------------+
| How do I show where a source comes from?      | procedures/external_lineage.md|
+-----------------------------------------------+-------------------------------+
| What is planned but not done?                 | planning/TODO.md              |
+-----------------------------------------------+-------------------------------+
| What was decided against, and why?            | planning/decisions.md         |
+-----------------------------------------------+-------------------------------+
| What might be worth doing someday?            | planning/ideas.md             |
+-----------------------------------------------+-------------------------------+
| What does this word mean (bind, target, ...)? | glossary.md                   |
+-----------------------------------------------+-------------------------------+
```

## Where the Labeled Tables Are

Ask for a table by its label.

Labels:

```
+--------------------+------------------------------+--------------------------+
| Label              | File                         | Section                  |
+--------------------+------------------------------+--------------------------+
| Targets:           | procedures/bundle_deploy.md  | What the Bundle Is       |
+--------------------+------------------------------+--------------------------+
| GUI or CLI:        | procedures/bundle_deploy.md  | Rules                    |
+--------------------+------------------------------+--------------------------+
| Workspaces:        | procedures/git_workflow.md   | Overview                 |
+--------------------+------------------------------+--------------------------+
| Branches and Tags: | procedures/git_workflow.md   | Overview                 |
+--------------------+------------------------------+--------------------------+
| Free jobs before   | procedures/bundle_deploy.md  | Worked Example: First    |
| binding:           |                              | Deploy to the Free       |
|                    |                              | Workspace                |
+--------------------+------------------------------+--------------------------+
| Environments:      | environments.md              | (top)                    |
+--------------------+------------------------------+--------------------------+
| Task value lists:  | jobs/ingest.md               | task_values              |
+--------------------+------------------------------+--------------------------+
| Questions:         | README.md                    | Which File Answers Which |
|                    |                              | Question                 |
+--------------------+------------------------------+--------------------------+
| Labels:            | README.md                    | Where the Labeled Tables |
|                    |                              | Are                      |
+--------------------+------------------------------+--------------------------+
```

## Folders in doc/

```
examples/bronze, examples/silver   settings files to copy from
examples/fetch_url                 fetch_url settings to copy from
examples/fetch_sftp                fetch_sftp settings to copy from
examples/fetch_rest                fetch_rest settings to copy from
examples/ps1                       upload scripts (one per source)
examples/apps                      a Databricks app example
examples/views                     SQL views over silver tables
examples/extra                     dashboard SQL and analysis notebooks
examples/lineage                   external lineage bodies (source URL -> bronze)
jobs/                              one doc per job in resources/
planning/                          decisions, ideas and TODO
tools/make_docx.py                 turns the docs into Word files:
                                   py doc/tools/make_docx.py --all <out folder>
                                   py doc/tools/make_docx.py <doc.md> <out.docx>
tools/make_slide.py                makes the releases-to-workspaces slides:
                                   py doc/tools/make_slide.py <out.pptx>
```
