# Release to Staging

How to put a commit that dev already ran into the staging workspace.
Staging runs a fixed tag, staging_v0.NN, made on a dev tag's commit;
see **git_workflow.md** for why, and **bundle_deploy.md** for how the bundle
works.

## Short Version (Every Release)

```
git tag staging_v0.01 dev_v0.004
git push origin staging_v0.01
git switch --detach staging_v0.01
databricks bundle deploy -t staging -p staging
git switch master
```

1. Label the commit dev ran with the next staging number. Never
tag master: it may hold newer, untested work.
2. Publish the label. The jobs pull from GitHub, so it must be
there.
3. Stand on the label: your files become exactly that commit, so
the deploy reads its YAML.
4. Deploy: the staging jobs now pull that commit.
5. Step off, back to master.

## Long Version (First Release to Staging)

Steps 0 to 4 and 7 are only for the first time.

0. Turn on the staging target. It's commented out in
**databricks.yml**. On a topic branch: uncomment it. It already
holds the jobs and the utility schemas and volumes; add schemas for
any datasets wanted in staging (staging_ catalogs), and add staging
to their settings' environments lists. Merge to master and release
that to dev (**release_dev.md**). The staging target must be in the
commit you tag, so step 5 tags that dev release.

1. CLI profile: **~/.databrickscfg** needs a staging profile holding
the staging host. Check it works:

```
databricks current-user me -p staging
```

Every databricks command below uses -p staging. Without it the CLI
uses the DEFAULT profile, which may be another workspace.

2. Repo URL: origin must be the https URL, since the jobs pull
from it (**bundle_deploy.md**, "What the Bundle Is").

GitHub access: if the repo is private, link a GitHub token in
the staging workspace (User settings > Linked accounts). The
workspace must be able to reach github.com.

3. Catalogs, made by hand: staging_utility, plus
staging_\<dataset> for each dataset added in step 0, each with a
workspace binding to staging only (**bundle_deploy.md** "First Deploy, New Workspace" step a).

4. Existing schemas or volumes: if any of staging's already exist,
bind them first (**bundle_deploy.md** "Bind Existing Schemas and Volumes"). Probably none.

5. Tag and push, on the dev tag from step 0:

```
git fetch origin --tags
git tag staging_v0.01 dev_v0.004
git push origin staging_v0.01
```

6. Deploy:

```
git switch --detach staging_v0.01
git status                                         (must be clean)
databricks bundle validate -t staging -p staging
databricks bundle plan -t staging -p staging       (expect 12 add, plus 2 per dataset, 0 change, 0 delete)
databricks bundle deploy -t staging -p staging     (rerun once if a new volume is "not found")
```

7. Source folders, one per dataset added in step 0:

```
databricks fs mkdir --profile staging dbfs:/Volumes/staging_utility/raw/files/<dataset>
```

8. Smoke test: upload a file and watch file_router, then ingest:

```
.\fred.ps1 -Profile staging -Catalog staging_utility
```

9. Verify: a staging job's Git commit (Job details > Git) equals

```
git rev-parse staging_v0.01
```

10. Switch back:

```
git switch master
```

## Roll Back

Deploy the previous staging tag the same way (here staging_v0.02
is bad, so back to staging_v0.01):

```
git switch --detach staging_v0.01
databricks bundle deploy -t staging -p staging
git switch master
```

No tag is moved or deleted. Fix forward with new tags.
