# Git Workflow

How a change moves from an idea to production. The framework repo holds
every job's code (**ingest/**, **file_router/**, **fetch_url/**, **fetch_sftp/** and
**fetch_rest/**), so one branch and one tag cover everything.

## Overview

```
topic branch --merge--> master --tag--> dev_v0.001 --tag--> staging_v0.01 --tag--> prod_v0.1
                          |
                          +--> free workspace pulls master
                               (or a topic branch while you test it)
```

Only topic branches and master are branches. Dev, staging and prod
run tags, like ClearCase labels that never move: to promote, make
a new tag on the same commit. Each work target's jobs pull the
exact commit you deploy from (git_commit: **${bundle.git.commit}**),
so you deploy from a checkout of the tag, and the job definitions
and the code are always the same commit.

Workspaces:

```
+-----------+----------------------+-----------------------------------+
| Workspace | Runs                 | Moved by                          |
+-----------+----------------------+-----------------------------------+
| free      | branch: master       | merging a topic into master       |
+-----------+----------------------+-----------------------------------+
| dev       | tag: dev_v0.NNN      | new tag on a master commit,       |
|           |                      | deployed from that tag            |
+-----------+----------------------+-----------------------------------+
| staging   | tag: staging_v0.NN   | new tag on a dev tag's commit,    |
|           |                      | deployed from that tag            |
+-----------+----------------------+-----------------------------------+
| prod      | tag: prod_v0.N       | new tag on a staging tag's        |
|           |                      | commit, deployed from it          |
+-----------+----------------------+-----------------------------------+
```

Branches and Tags:

```
+---------------+--------------------------------------------------+
| Name          | Means                                            |
+---------------+--------------------------------------------------+
| topic         | One change in progress. Short name saying what   |
|               | it does, e.g. fred-html-guard. Deleted after     |
|               | merging.                                         |
+---------------+--------------------------------------------------+
| master        | Everything tested and accepted.                  |
+---------------+--------------------------------------------------+
| dev_v0.NNN    | A commit on master, for dev. Three digits.       |
+---------------+--------------------------------------------------+
| staging_v0.NN | A commit dev already ran. Two digits.            |
+---------------+--------------------------------------------------+
| prod_v0.N     | A release: a commit staging already ran. One     |
|               | digit.                                           |
+---------------+--------------------------------------------------+
```

The numbers count up on their own; staging_v0.03 can be the same
commit as dev_v0.017.

## Rules

1. Every change starts on its own topic branch.

2. Topic branches merge into master only.

3. A tag is never moved, deleted or reused. A fix is a new tag.

4. Dev tags are made on master. Staging tags are made on a dev
tag, prod tags on a staging tag, never on master: by then
master may hold newer, untested work.

5. Deploy dev, staging and prod only from a clean checkout of
their tag (git switch --detach **\<tag>**; git status shows
nothing). The jobs pull the commit you deploy from, so a
deploy from master or a topic would point them there, and
uncommitted edits would deploy without being in the tag.
These deploys use the CLI: the workspace Git folder (and its
Deployments panel) can only check out branches.

6. Push the tag before deploying. The jobs pull the commit from
GitHub, so it must be there.

7. Don't make a dev tag until the topic branch is deleted (end of
"Accept It into Master"). "Promote to Dev" starts with a check.

Tags and master are pushed to origin, the repo the jobs pull
from.

## Start a Change

```
git switch master
git pull
git switch -c fred-html-guard
```

Commit as you go, and push the branch so Databricks can pull it:

```
git push -u origin fred-html-guard
```

## Test It in Free

Point free's jobs at the branch. Push the branch first (the jobs
pull it from GitHub), set free's git_branch in **databricks.yml**
to the branch (don't commit that), then:

```
databricks bundle deploy -t free
```

Details: **bundle_deploy.md** "Test a Topic Branch in the Free Workspace".

Drop a file in the inbox, or click Run now on a job, and check the
run.

Fix and push again as needed; the job pulls the latest commit on
the branch every run.

## Accept It into Master

Put free's git_branch back first:

```
git restore databricks.yml
```

Then merge:

```
git switch master
git pull
git merge fred-html-guard
git push
```

Deploy free, so its jobs pull master again:

```
databricks bundle deploy -t free
```

Free's jobs pull master, so they run the change from their next
run.

```
Delete the topic branch:
```

```
git branch -d fred-html-guard
git push origin --delete fred-html-guard
```

## Promote to Dev

First check that no merged topic branches are left over:

```
git fetch --prune origin
git branch -r --merged origin/master
```

The list should show only origin/master (plus origin/HEAD). Any
other name is a merged topic that "Accept It into Master" didn't delete; delete it
before going on:

```
git push origin --delete <topic>
git branch -d <topic>
```

Topics still in progress (not merged yet) don't appear in this
list and can stay.

Then pick the next dev number, tag master, and push the tag:

```
git fetch origin --tags
git tag --list "dev_v*" --sort=v:refname
git tag dev_v0.004 origin/master
git push origin dev_v0.004
```

Deploy dev from the tag, then go back to master:

```
git switch --detach dev_v0.004
git status
databricks bundle deploy -t dev
git switch master
```

Dev's next run uses it.

## Promote to Staging

Once dev has run it and it looks right, tag the same commit:

```
git tag --list "staging_v*" --sort=v:refname
git tag staging_v0.02 dev_v0.004
git push origin staging_v0.02
```

```
git switch --detach staging_v0.02
git status
databricks bundle deploy -t staging
git switch master
```

Staging now runs exactly what dev ran.

## Release to Prod

Once staging has run it and it looks right:

```
git tag --list "prod_v*" --sort=v:refname
git tag prod_v0.2 staging_v0.02
git push origin prod_v0.2
```

```
git switch --detach prod_v0.2
git status
databricks bundle deploy -t prod
git switch master
```

Prod's next run uses the release.

## Roll Back

Deploy the previous tag again, the same way (dev, staging or
prod):

```
git switch --detach prod_v0.1
databricks bundle deploy -t prod
git switch master
```

No tag is moved or deleted. Then fix the problem as a new topic
("Start a Change") and promote it as new tags.

## What Is Each Workspace Running?

Free runs master:

```
git log -1 --oneline origin/master
```

Dev, staging and prod run whatever commit was last deployed. Its
hash is in each job's Job details > Git, or in git_source.git_commit
from **databricks jobs get -p \<profile> \<job id>**. To name its tags:

```
git fetch origin --tags
git tag --points-at <commit>
```

After a rollback, that is not the newest tag.

What is in master but not yet in a dev tag:

```
git log --oneline dev_v0.004..origin/master
```

