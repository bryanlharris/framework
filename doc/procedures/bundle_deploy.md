# Bundle Deploy

How the ingest, file_router, fetch_url, fetch_sftp and fetch_rest jobs
get created and updated in each workspace with the Databricks Asset
Bundle in this repo.

## What the Bundle Is

```
framework/
  databricks.yml             the bundle: targets table, one per workspace
  resources/
    ingest.job.yml           ingest job definition
    file_router.job.yml      file_router job definition
    fetch_url.job.yml        fetch_url job definition
    fetch_sftp.job.yml       fetch_sftp job definition
    fetch_rest.job.yml       fetch_rest job definition
```

The bundle defines the jobs (tasks, triggers, parameters, git
branch or tag), and in free and dev the schemas and volumes.
The jobs always pull their code from GitHub when they run.
Deploying never copies code into the workspace.

Each target in **databricks.yml** is one workspace, named by a CLI
profile (workspace.profile), and states everything that differs
there. The host is in that profile in **~/.databrickscfg**, never in
the repo.

Targets:

```
+---------+----------------------+----------------+-------------+--------------------+
| Target  | profile              | catalog_prefix | environment | Git                |
+---------+----------------------+----------------+-------------+--------------------+
| free    | DEFAULT              | (empty)        | free        | branch: master     |
| dev     | dev                  | dev_           | dev         | deployed tag       |
| staging | staging (commented)  | staging_       | staging     | deployed tag       |
| prod    | prod (commented)     | (empty)        | prod        | deployed tag       |
+---------+----------------------+----------------+-------------+--------------------+
```

free's and dev's schemas and volumes are bundle-managed
(free's were bound, "Bind Existing Schemas and Volumes"); catalogs are made by hand
everywhere. staging and prod are commented out. There is no
default target: every bundle command names one with -t, so a
command can't land on a workspace by accident.

The jobs pull from origin: git_url is **${bundle.git.origin_url}**,
the origin URL of the checkout you deploy from, so no repo URL is
written in the code. It must be the https form
(**https://github.com/\<owner>/framework**); Databricks can't use the
SSH form (git@github.com:...). If your origin is SSH, switch it to
https and keep using SSH underneath, once per clone:

```
git remote set-url origin https://github.com/<owner>/framework
git config --local url."git@github.com:".insteadOf "https://github.com/"
```

Both change only .git/config, which is never pushed. Check with:

```
databricks bundle validate -t <target> -o json
```

(**bundle.git.origin_url** must start with **https://**). git_provider is
gitHub.

Trigger folders and the router's catalog are built from
catalog_prefix, so they always agree with each other.

Commands run from the top of the framework repo, where
**databricks.yml** is. The CLI takes the host and credentials from
the target's profile in **~/.databrickscfg**, so bundle commands need
no -p. Plain commands (fs, jobs, secrets) do: without -p they use
the DEFAULT profile, which may be another workspace.

## Rules

1. Once a workspace's jobs are deployed by the bundle, change them
in the YAML files and deploy. Edits made in the job UI are
overwritten by the next deploy.

2. Never deploy to a workspace whose jobs already exist without
binding them first (see "First Deploy, Jobs Already Exist").
Otherwise the deploy creates a second copy of each job, and both
copies' triggers fire on every upload.

3. Don't use "databricks bundle destroy". It deletes the jobs.

4. The Deploy button in the workspace's Deployments panel (in the
framework Git folder) works too. It shares the CLI's deploy
state and updates the same jobs. Before using it, pull the Git
folder: it deploys whatever the folder has checked out. Keep
the local CLI at least as new as the panel's (shown in its
Deploy dialog); an older CLI can't read state a newer one
wrote. The panel ignores a target's workspace.profile and uses
the workspace it runs in, and reads git_url
(**${bundle.git.origin_url}**) from the Git folder's origin (both
checked 2026-10-24).

GUI or CLI:

```
+--------------------------+----------------------------+----------------+
| Deploy                   | GUI (Deployments panel)    | CLI            |
+--------------------------+----------------------------+----------------+
| free (master)            | yes: Git folder on master  | yes            |
+--------------------------+----------------------------+----------------+
| dev, staging, prod (tag) | no: Git folder can't check | yes            |
|                          | out tags                   |                |
+--------------------------+----------------------------+----------------+
| bind or unbind           | no                         | yes            |
| (the bind and unbind     |                            |                |
| sections below)          |                            |                |
+--------------------------+----------------------------+----------------+
| a deploy that deletes    | only "Deploy with          | --auto-approve |
|                          | auto-approval"             |                |
+--------------------------+----------------------------+----------------+
```

Not checked: whether the panel can show a plan, or whether it
deploys uncommitted edits in the Git folder.

## Check Before Any Deploy

```
databricks bundle validate -t <target>
```

Expected output ends with:

```
Found 1 warning
Warning: There are no files to sync ...
```

That warning is expected: **databricks.yml** tells the bundle not to
upload the checkout, because the jobs pull from GitHub. Anything
else (an error, or a second warning) must be fixed first.

To see exactly what will be deployed, with every variable filled
in:

```
databricks bundle validate -t <target> -o json
```

## First Deploy, Jobs Already Exist (Bind, Then Deploy)

Use when a workspace already has the ingest and file_router jobs,
made by hand (the ingest job may still be called framework there;
the deploy renames it). Binding links the bundle to those exact
jobs, so the deploy updates them in place: same job IDs, URLs and
run history. fetch_url, fetch_sftp and fetch_rest are new, so they
aren't bound; the deploy creates them.

a. Find the job IDs (Jobs & Pipelines > job > the number in the
URL, or):

```
databricks jobs list -p <profile>
```

b. Bind each job (asks to confirm; answer y):

```
databricks bundle deployment bind ingest      <ingest job id>      -t <target>
databricks bundle deployment bind file_router <file_router job id> -t <target>
```

c. Validate and deploy:

```
databricks bundle validate -t <target>
databricks bundle deploy -t <target>
```

d. Confirm the two bound jobs are still the same IDs:

```
databricks bundle summary -t <target>
```

See the worked example below for the free workspace.

## First Deploy, New Workspace (No Jobs Yet)

a. The workspace must already have its catalogs, made by hand
(the bundle creates the schemas and volumes in them). For dev:

```
dev_utility, dev_edsm
```

(staging and prod: utility only, plus the catalogs of any
datasets added to them; see **doc/environments.md**).

Give each catalog a workspace binding to its own workspace
only: Catalog Explorer > catalog > Workspaces tab, turn off
"All workspaces have access", assign this workspace. Then no
other workspace can read or write it (e.g. dev can't touch
prod's fred). Check the tab shows only this workspace.

If any of the target's schemas or volumes already exist
there, bind them to the bundle first ("Bind Existing Schemas and Volumes"; a different
"bind" from the workspace binding).

After the first deploy (step d), make one folder per source,
from the CLI:

```
databricks fs mkdir --profile dev dbfs:/Volumes/dev_utility/raw/files/<catalog>
```

b. Give the CLI a profile for that workspace, named like the
target (dev, staging or prod), once:

```
databricks auth login --host https://<workspace host> -p dev
```

The host goes in **~/.databrickscfg** only, never in the repo.

c. Make sure the target in **databricks.yml** is uncommented.

d. Validate and deploy, from a clean checkout of the target's
tag (**git_workflow.md**, "Promote to Dev" through "Release to Prod"). No bind for the jobs; the
deploy creates all five, and the schemas and volumes. A
brand-new volume can be "not found" for a moment, so a first
deploy may need a rerun.

```
git switch --detach <tag>
databricks bundle validate -t <target>
databricks bundle deploy -t <target>
git switch master
```

## Everyday Change to a Job

Edit **resources/\<job>.job.yml** (or a target in **databricks.yml**), then:

```
databricks bundle validate -t <target>
databricks bundle deploy -t <target>
```

Commit the YAML change like any other change (see
**git_workflow.md**).

That is for free. Dev, staging and prod get a job change
only with a new tag: the change goes through master, then is
promoted as in **git_workflow.md**, "Promote to Dev" through "Release to Prod". Never deploy them
from master or a topic: their jobs pull the commit you deploy
from.

## Test a Topic Branch in the Free Workspace

(Used by **git_workflow.md**, "Test It in Free" and "Accept It into Master".)

Point free's jobs at the branch for a while. Push the branch first
(the jobs pull it from GitHub), set free's git_branch in
**resources/free.local.yml** to the branch name, and deploy:

```
databricks bundle deploy -t free
```

Run the jobs and check them. When done, set git_branch back to
master and deploy again:

```
databricks bundle deploy -t free
```

## Unbind (Give a Job Back to Hand Management)

```
databricks bundle deployment unbind ingest      -t <target>
databricks bundle deployment unbind file_router -t <target>
```

The jobs keep running exactly as last deployed; the bundle just
stops managing them. UI edits are safe again.

## Rename a Job's Bundle Key (Keeps the Job ID and Run History)

Changing a job's key in **resources/\*.job.yml** looks to the bundle
like one job removed and another added: the next deploy would
delete the job, history included, and create a new one. Hand the
job to the new key instead. In this order, with no deploy until d:

a. databricks bundle deployment unbind \<old key> -t **\<target>**  
b. Rename the key (and name:, and the file) in **resources/**, and the
key in every target override in **databricks.yml**.  
c. databricks bundle deployment bind \<new key> \<job id> -t **\<target>**  
d. databricks bundle plan -t **\<target>**
It must say "update jobs.\<new key>" with 0 to add and 0 to
delete. Anything else: stop. Then deploy.

Done in free on 2026-10-23: framework -> ingest, job ID
179063231760295.

## Worked Example: First Deploy to the Free Workspace

Done: the free workspace's jobs are bound and bundle-managed. Kept
as a record and as the pattern for the work workspaces. The
framework job below has since been renamed ingest ("Rename a Job's Bundle Key");
use ingest as the key in new workspaces.

Situation before: the free workspace had both jobs, made by hand.

Free jobs before binding:

```
+-------------+------------------+-----------------------------+
| Bundle key  | Job ID           | Trigger folder              |
+-------------+------------------+-----------------------------+
| framework   | 179063231760295  | /Volumes/utility/raw/files/ |
| file_router | 1011420299706363 | /Volumes/utility/raw/inbox/ |
+-------------+------------------+-----------------------------+
```

Added later: fetch_url (763610887760027), fetch_sftp
(218443793259980) and fetch_rest (64242777793505), all on a daily
schedule with no trigger folder, were created by a plain deploy,
with no bind.

Step 1. Start from an up-to-date master:

```
git switch master
git pull
```

Step 2. Check the CLI login works (token needs the workspace
scope):

```
databricks current-user me
```

Step 3. Validate. Expect only the "no files to sync" warning:

```
databricks bundle validate -t free
```

Step 4. Save the current job definitions, outside the repo, in
case you want to compare or restore:

```
databricks jobs get 179063231760295  > $HOME\Downloads\framework_before.json
databricks jobs get 1011420299706363 > $HOME\Downloads\file_router_before.json
```

Step 5. Bind both jobs (answer y to each):

```
databricks bundle deployment bind framework   179063231760295  -t free
databricks bundle deployment bind file_router 1011420299706363 -t free
```

Step 6. Deploy:

```
databricks bundle deploy -t free
```

Step 7. Confirm the bundle manages the same two job IDs:

```
databricks bundle summary -t free
```

Step 8. Check each job in the UI (Jobs & Pipelines):
- a notice that the job is managed by a bundle
- framework: trigger **/Volumes/utility/raw/files/**,
parameters catalog_prefix (empty) and environment = free,
Git branch master
- file_router: trigger **/Volumes/utility/raw/inbox/**,
parameter catalog = utility, Git branch master,
file **file_router/file_router.py**

Step 9. Test end to end: upload a file (e.g. .\fdic.ps1 from
**doc/examples/ps1**). The router should route it within a couple of
minutes, then the framework should start on its own.

If something is wrong: unbind both jobs ("Unbind") and restore a
saved definition with

```
databricks jobs reset --json @<file with job_id and new_settings>
```

using the settings from the step 4 files.

## Who Deleted a Schema or Volume? (Audit Log)

Every delete is logged in **system.access.audit** with the user, IP
address and tool, whether it came from a bundle deploy, a bundle
destroy, the UI or SQL. Logs arrive with a delay of minutes to
an hour. In a SQL editor:

```
select event_time, action_name,
       user_identity.email, source_ip_address, user_agent,
       request_params.full_name_arg as object,
       request_params.force          as force
from system.access.audit
where event_date >= current_date() - 7
  and service_name = 'unityCatalog'
  and action_name in ('deleteSchema', 'deleteVolume',
                      'deleteTable', 'deleteCatalog')
order by event_time desc
```

```
Reading user_agent:
  cmd/bundle_deploy ... upstream/dabs-in-workspace
                       a deploy from the workspace Deployments panel
  cmd/bundle_deploy, cmd/bundle_destroy (os/windows etc.)
                       the CLI, from someone's machine
  agent/claude-code    run by Claude Code
  Mozilla/...          clicked in the browser UI
```

force = true means the schema was dropped with everything in it
(the bundle always deletes that way); a table dropped that way
has no event of its own. The approval click (auto-approval or
--auto-approve) isn't logged, only the delete and who did it.

## Bind Existing Schemas and Volumes

Use when a target's schemas and volumes already exist (made by
hand) and are being added to its resources in **databricks.yml**.
Without the bind, the deploy tries to create them. Catalogs stay
manual.

a. Add the schemas: and volumes: entries to the target, each with
lifecycle.prevent_destroy: true (copy free's), except in dev.

b. Bind each one to its full name:

```
databricks bundle deployment bind utility_raw  utility.raw       -t <target>
databricks bundle deployment bind raw_inbox    utility.raw.inbox -t <target>
...
```

c. databricks bundle plan -t **\<target>**
It must say 0 to add and 0 to delete. Anything else: stop.
Then deploy.

Done in free on 2026-10-24: schemas utility.raw, utility.bronze,
utility.silver, edsm, fdic and fred bronze and silver; volumes
**utility.raw.inbox**, **utility.raw.files**, **utility.bronze.state**,
**utility.silver.state**.

## Releasing to Dev, Staging and Prod

Dev, staging and prod run tags (dev_v0.NNN, staging_v0.NN,
prod_v0.N) that never move. Their jobs use
git_commit: **${bundle.git.commit}**: they pull the exact commit
checked out when you deploy. So deploy from a clean checkout of
the tag, and the job definitions and the code are the same
commit. CLI only: the workspace Git folder can't check out tags.
Steps: **git_workflow.md**, "Promote to Dev" through "Roll Back".
