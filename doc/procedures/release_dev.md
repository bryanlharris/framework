# Release to Dev

How to put a commit from master into the dev workspace. Dev runs a
fixed tag, dev_v0.NNN; see **git_workflow.md** for why, and
**bundle_deploy.md** for how the bundle works.

## Short Version (Every Release)

```
git tag dev_v0.001 master
git push origin dev_v0.001
git switch --detach dev_v0.001
databricks bundle deploy -t dev -p dev
git switch master
```

1. Label master's commit with the next dev number. Local only.
2. Publish the label. The jobs pull from GitHub, so it must be
there.
3. Stand on the label: your files become exactly that commit, so
the deploy reads its YAML.
4. Deploy: the dev jobs now pull that commit.
5. Step off, back to master.

## Long Version (First Release to Dev)

Steps 1 to 4 and 7 are only for the first time.

0. Up to date:

```
git switch master
git pull
```

1. CLI profile: **~/.databrickscfg** needs a dev profile holding the
dev host (staging and prod get their own). The dev target names
that profile, so the host is never in the repo. Check it works:

```
databricks current-user me -p dev
```

Every databricks command below uses -p dev. Without it the CLI uses
the DEFAULT profile, which may be another workspace.

2. Repo URL: origin must be the https URL, since the jobs pull
from it (**bundle_deploy.md**, "What the Bundle Is").

GitHub access: if the repo is private, link a GitHub token in
the dev workspace (User settings > Linked accounts). The workspace
must be able to reach github.com.

3. Catalogs, made by hand: dev_utility and dev_edsm, each
with a workspace binding to dev only (**bundle_deploy.md** "First Deploy, New Workspace" step a).

4. Existing schemas or volumes: if any of dev's already exist, bind
them first (**bundle_deploy.md** "Bind Existing Schemas and Volumes"). Probably none.

5. Tag and push:

```
git tag dev_v0.001 master
git push origin dev_v0.001
```

6. Deploy:

```
git switch --detach dev_v0.001
git status                                 (must be clean)
databricks bundle validate -t dev -p dev
databricks bundle plan -t dev -p dev       (expect 14 add, 0 change, 0 delete)
databricks bundle deploy -t dev -p dev     (rerun once if a new volume is "not found")
```

7. Source folders:

```
databricks fs mkdir --profile dev dbfs:/Volumes/dev_utility/raw/files/edsm
```

8. Smoke test: run fetch_url, then watch file_router and ingest pick up the edsm file:

```
databricks bundle run fetch_url -t dev -p dev
```

9. Verify: a dev job's Git commit (Job details > Git) equals

```
git rev-parse dev_v0.001
```

10. Switch back:

```
git switch master
```
