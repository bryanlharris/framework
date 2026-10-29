# Glossary

Words used across these docs, especially ones that sound alike.

## Workspaces and Targets

**workspace**: One Databricks workspace: free (personal), or dev, staging and prod at work.

**target**: One entry under targets: in **databricks.yml**. Usually one per workspace; test is a second target in the free workspace. Picked with -t **\<target>**.

**environment**: free, test, dev, staging or prod. A job parameter set by the target; picks which settings files run (their "environments" list). **environments.md**.

**catalog_prefix**: What every catalog name starts with in a target: dev_, staging_, test_, or empty in free and prod. Makes dev_fred in dev and fred in prod. **environments.md**.

**free**: The personal workspace, and its target. Runs master.

**test**: A second target in the free workspace: "[test]" jobs, test_ catalogs. Runs the checked-out branch, for trying a topic without touching free.

## The Bundle

**bundle**: **databricks.yml** plus **resources/\*.yml**: the jobs, schemas and volumes, per target.

**resource**: Something the bundle creates and manages: a job, schema or volume. Not catalogs, and not the code.

**deploy**: databricks bundle deploy -t **\<target>**: makes the workspace match the YAML. Reads the job definitions from your checkout; uploads no code.

**job definition**: A job's tasks, triggers, parameters and git ref, as written in the YAML. Comes from the checkout at deploy time.

**code**: The notebooks and Python the jobs run. Pulled from GitHub on every run (git_source), at the target's git ref.

**git ref**: Which code a target's jobs pull: branch: master (free), the checked-out branch (test), or the deployed commit (dev, staging, prod).

**bind**: databricks bundle deployment bind: hand an existing job, schema or volume to the bundle, so the deploy updates it instead of making a second one. **procedures/bundle_deploy.md** "First Deploy, Jobs Already Exist" and "Bind Existing Schemas and Volumes". Not the same as a workspace binding.

**unbind**: The reverse: the bundle stops managing it; the object stays. **procedures/bundle_deploy.md** "Unbind".

**prevent_destroy**: A lifecycle setting on every schema and volume except dev's: deploy refuses to delete it.

**plan**: databricks bundle plan -t **\<target>**: what a deploy would add, change or delete, without doing it. It lists bundle keys (schemas.edsm_bronze); py tools/bundle_plan.py **\<target>** shows the same plan by real name (dev_edsm.bronze).

## Unity Catalog

**catalog**: Top level of names (fred, dev_utility). Always made by hand, never by the bundle.

**workspace binding**: A catalog setting: only the named workspaces can use it. Every work catalog is bound to its own workspace only, by hand in the UI. Not the same as a bundle bind.

**schema**: Second level (fred.bronze). Bundle-managed.

**volume**: A folder of files in a schema (**utility.raw.inbox**). Bundle-managed.

**inbox**: **utility.raw.inbox**: uploads and fetch jobs drop files here; file_router moves them on.

**raw files**: **utility.raw.files/\<source>/**: where file_router puts files. A new file triggers the ingest job.

**state volumes**: **utility.bronze.state** and **utility.silver.state**: checkpoints, schemas and bad records.

**source**: A folder under raw files, named after its catalog (fred, fdic, edsm).

## Git

**topic branch**: One change in progress (e.g. fred-html-guard). Merged into master, then deleted.

**master**: Everything tested and accepted. The only long-lived branch.

**tag**: A fixed label on one commit, never moved. Dev, staging and prod run tags: dev_v0.NNN, staging_v0.NN, prod_v0.N. **procedures/git_workflow.md**.

**promote**: Make the next environment's tag on the same commit (staging_v0.02 on dev_v0.004), then deploy from it.

**origin**: The GitHub repo the jobs pull from. Master and tags are pushed there.

**Git folder**: A clone of the repo inside a workspace. Can check out branches only, not tags.

**Deployments panel**: The Deploy button in a Git folder: deploys what the folder has checked out. Works for free and test, not for tag deploys. **procedures/bundle_deploy.md** "GUI or CLI".

## Jobs

**file_router**: Moves files from the inbox to raw files, by name ({source}_{filename}.ext). **jobs/file_router.md**.

**ingest**: Loads raw files into bronze, then silver tables. **jobs/ingest.md**.

**fetch jobs**: fetch_url, fetch_sftp and fetch_rest: daily, put files in the inbox. Paused in free, test and dev. **jobs/fetch_\*.md**.

**settings file**: One JSON per table (**ingest/settings/**) or per fetch (**fetch_\*/settings/**). **api.md**.
