# Release to Prod

How to put a commit that staging already ran into the prod
workspace. Prod runs a fixed tag, prod_v0.N, made on a staging tag's
commit; see **git_workflow.md** for why, and **bundle_deploy.md** for how the
bundle works.

Prod's catalogs have no prefix (utility, and one per dataset, such as
fred), the same names as free's. Two things keep them apart: each prod catalog's workspace
binding, and -p prod on every command. Check both every time.

## Short Version (Every Release)

```
git tag prod_v0.1 staging_v0.01
git push origin prod_v0.1
git switch --detach prod_v0.1
databricks bundle plan -t prod -p prod
databricks bundle deploy -t prod -p prod
git switch master
```

1. Label the commit staging ran with the next prod number. Never
tag master or a dev tag.
2. Publish the label. The jobs pull from GitHub, so it must be
there.
3. Stand on the label: your files become exactly that commit, so
the deploy reads its YAML.
4. Plan: read it. It should show the same job changes staging got
from this commit, and 0 to delete. Anything else: stop.
5. Deploy: the prod jobs now pull that commit.
6. Step off, back to master.

## Long Version (First Release to Prod)

Steps 0 to 4 and 7 are only for the first time.

0. Turn on the prod target. It's commented out in **databricks.yml**.
On a topic branch: uncomment it. It already holds the jobs and
the utility schemas and volumes; add schemas for any datasets
wanted in prod (unprefixed catalogs, such as fred), and add prod to
their settings' environments lists. Merge to master, then release that through dev and staging
(**release_dev.md**, **release_staging.md**). The prod target must be in
the commit you tag, so step 5 tags that staging release.

1. CLI profile: **~/.databrickscfg** needs a prod profile holding the
prod host. Check it works, and that it's the prod workspace:

```
databricks current-user me -p prod
databricks catalogs list -p prod
```

Every databricks command below uses -p prod. Without it the CLI
uses the DEFAULT profile, which may be another workspace.

2. Repo URL: origin must be the https URL, since the jobs pull
from it (**bundle_deploy.md**, "What the Bundle Is").

GitHub access: if the repo is private, link a GitHub token in
the prod workspace (User settings > Linked accounts). The
workspace must be able to reach github.com.

3. Catalogs, made by hand: utility, plus one per dataset added in
step 0, each with a workspace binding to prod only
(**bundle_deploy.md** "First Deploy, New Workspace" step a).

4. Existing schemas or volumes: if any of prod's already exist,
bind them first (**bundle_deploy.md** "Bind Existing Schemas and Volumes").

5. Tag and push, on the staging tag from step 0:

```
git fetch origin --tags
git tag prod_v0.1 staging_v0.01
git push origin prod_v0.1
```

6. Deploy:

```
git switch --detach prod_v0.1
git status                                   (must be clean)
databricks bundle validate -t prod -p prod
databricks bundle plan -t prod -p prod       (expect 12 add, plus 2 per dataset, 0 change, 0 delete)
databricks bundle deploy -t prod -p prod     (rerun once if a new volume is "not found")
```

7. Source folders, one per dataset added in step 0:

```
databricks fs mkdir --profile prod dbfs:/Volumes/utility/raw/files/<dataset>
```

8. First run: don't upload test data to prod. Wait for the first
real file (or the fetch jobs' first scheduled run) and watch
file_router, then ingest.

9. Verify: a prod job's Git commit (Job details > Git) equals

```
git rev-parse prod_v0.1
```

10. Switch back:

```
git switch master
```

## Roll Back

Deploy the previous prod tag the same way (here prod_v0.2 is bad,
so back to prod_v0.1):

```
git switch --detach prod_v0.1
databricks bundle plan -t prod -p prod
databricks bundle deploy -t prod -p prod
git switch master
```

No tag is moved or deleted. Fix the problem as a new topic and
promote it through dev and staging as new tags.
