# Decisions

Settled questions, with the options considered, so they don't get
reopened. Revisit only if something here changes.

## 1. Schemas and Volumes: Static Bundle YAML; Catalogs by Hand (2026-10-24, Revises 2026-10-23)

First decided 2026-10-23: everything manual. Revisited after
trying it in the test target (**databricks.yml**, target test).

Decision:
- Schemas and volumes can be bundle resources, written out as
static YAML per target, each with lifecycle.prevent_destroy:
true. The test target is the working example. Applied to free
on 2026-10-24 by binding the existing schemas and volumes
(**procedures/bundle_deploy.md** "Bind Existing Schemas and Volumes"). The dev target has them in the
YAML too; any that already exist there need the same bind first, or the
deploy tries to create them. staging and prod stay commented
out, with no resources, until they are wanted.
- Catalogs stay manual. In free the bundle can't create them
(Default Storage catalogs only via UI or SQL); at work they'd
need storage_root and are admin territory anyway. Each work
catalog also needs a workspace binding (only its own workspace
can use it), a safety setting best done and checked by hand
in the UI.
- No Python loader that derives schemas from **ingest/settings**.

Why static YAML is acceptable after all:
- Deleting needs three deliberate acts: remove prevent_destroy,
remove the resource, and approve. prevent_destroy blocks even
bundle destroy (the whole destroy aborts). Without it, a
deploy that deletes or recreates a schema or volume still
needs approval: the panel's normal Deploy refuses ("console
does not support prompting"); only "Deploy with auto-approval"
or --auto-approve goes through.
- Once approved it really deletes: a schema holding a table was
dropped with the table, no "not empty" refusal. So never use
auto-approval casually.
- The YAML is verbose for people to edit, but Claude edits it.
- Dev is the exception (2026-10-25): no prevent_destroy, so
**databricks bundle destroy -t dev** wipes its jobs, schemas
(tables included) and volumes for a fresh start. A deploy that
drops a dev schema still asks for approval.
- Every delete is attributed in **system.access.audit** (checked
2026-10-24): deleteSchema with the user, IP and user_agent;
panel deploys show upstream/dabs-in-workspace, Claude Code
shows agent/claude-code. The bundle deletes with force=true
(cascade), so a dropped table has no event of its own. The
approval click isn't logged. Query: **procedures/bundle_deploy.md**
"Who Deleted a Schema or Volume?".

Why no Python loader:
- Retiring a source (or removing an environment from its
settings) makes every deploy plan to drop its schemas until
they're unbound, per target.
- It needs a venv with databricks-bundles, and python: is
top-level, so it runs for every target; panel support is
unclear.
- Static YAML was good enough in practice.

Practical notes from the test target:
- presets.name_prefix also renames schemas (raw -> test_raw); use
a job-name variable (job_name_prefix) instead.
- A job's file-arrival trigger needs its volume to exist; write
the trigger url as a reference to the volume resource so the
volume is created first. A brand-new volume can still be "not
found" for a moment, so a first deploy may need a rerun.
- Two targets on one workspace need default: true on one, or CLI
commands in the repo need -t. Later (2026-10-24) the default was
dropped: every command names its target with -t.
- Test's jobs pull **${bundle.git.branch}**, the checked-out branch;
a --var override was dropped because a panel deploy without
it silently pointed the jobs at master.

## 2. Dev, Staging and Prod Run Fixed Tags, Deployed from the Tag (2026-10-24)

Decision:
- Only topic branches and master are branches. Dev, staging and
prod run tags that never move, like ClearCase labels:
dev_v0.NNN (3 digits), staging_v0.NN (2), prod_v0.N (1). To
promote, make a new tag on the same commit.
- Their jobs use git_commit: **${bundle.git.commit}**, and each is
deployed from a clean checkout of its tag, from the CLI. Steps:
**procedures/git_workflow.md**.

Why:
- The job definitions and the code always come from the same
commit. With a tag written in the YAML, the deploy reads the
definitions from master while the jobs pull older code from
the tag.
- A tag can't move by mistake; a rollback is a deploy of the
previous tag.

Options not taken:
- Moving dev and staging branches: they work in the panel, but
they are labels that move.
- git_tag written in the YAML: works in the panel, but
definitions and code come from different commits.
- git_tag: ${var.release_tag}: shows the tag name in the job UI,
but needs the right --var on every deploy.

Cost:
- No panel deploys for these targets: the workspace Git folder
can only check out branches (checked 2026-10-24).
- Job pages show a commit hash; git tag --points-at **\<hash>** names
it.
- A deploy from the wrong checkout points the jobs there. Hence
the rule: clean tag checkout only.

## 3. Docs Are Markdown, with ASCII Tables in Code Blocks (2026-10-24)

Decision:
- **doc/** is .md: headings are #, prose is plain Markdown, and every
table, diagram and command block sits in a ``` code block, drawn
in ASCII as before.

Why:
- GitHub and the workspace render .md; the workspace's text editor
is awkward for .txt.
- Markdown tables can't have row divider lines or multi-line cells;
the ASCII tables keep both, and look the same in the terminal.

## 4. file_router Never Makes Source Folders (2026-10-25)

Decision:
- The folders under **raw/files** are the list of sources. file_router
reads that list and never adds to it; a file whose name matches no
folder goes to **raw/inbox/bad_files/**. A person makes the folder
when adding the source (**doc/procedures/add_table.md** step 3).

Why:
- Typos stay visible. If file_router made folders,
**frde_fedfunds.csv** would quietly make a frde source and sit in
**raw/files/frde/**, where nothing loads it. Instead it goes to
bad_files and shows in the run's report.
- Making the folder goes with adding the bronze settings, so a file
that reaches **raw/files** is one the ingest job expects.

Cost:
- One more step per new source: make the folder before the first
upload or fetch.
