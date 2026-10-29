"""Show what a bundle deploy would do, by real name.

bundle plan lists resource keys (schemas.edsm_bronze); this prints the name
each lands on (dev_edsm.bronze), grouped by type, with the fields a change
would touch. Jobs hang from their workspace, schemas from their catalog and
volumes from their schema.
-v adds the bundle keys. Read only: it runs bundle validate and plan, never
deploy.

Usage: py tools/bundle_plan.py <target> [-v]      e.g. py tools/bundle_plan.py dev
"""
import json
import re
import shutil
import subprocess
import sys

ACTIONS = {'skip': 'unchanged', 'update': 'change'}
LAYERS = {'raw': 0, 'bronze': 1, 'silver': 2, 'gold': 3}
VERBOSE = ('-v', '--verbose', '-verbose')
COMMIT = 'git_source.git_commit'
# Box lines on a console; plain ASCII when piped, since a pipe or file may not
# be UTF-8 (PowerShell pipes show box lines as junk).
if sys.stdout.isatty():
    MID, LAST, BAR = '├──', '└──', '│  '
else:
    MID, LAST, BAR = '|--', '`--', '|  '


def cli(*args):
    exe = shutil.which('databricks')
    if not exe:
        sys.exit('databricks CLI not found on PATH')
    run = subprocess.run([exe, 'bundle', *args, '-o', 'json'],
                         capture_output=True, text=True, encoding='utf-8')
    if run.returncode:
        sys.exit(run.stderr.strip() or run.stdout.strip())
    return json.loads(run.stdout)


def workspace(config):
    """The target's workspace, by profile name."""
    return f"workspace {config.get('workspace', {}).get('profile', '')}".strip()


def state(entry):
    """The resource as it will be (create, change) or as it is (delete, unchanged)."""
    new = entry.get('new_state', {}).get('value')
    return new if new is not None else entry.get('remote_state', {})


def tags(commit):
    """ (dev_v0.001) when local tags point at the commit; jobs store only the commit."""
    run = subprocess.run(['git', 'tag', '--points-at', commit],
                         capture_output=True, text=True, encoding='utf-8')
    found = run.stdout.split() if run.returncode == 0 else []
    return f" ({', '.join(found)})" if found else ''


def git_ref(s, entry):
    """A job's code: the commit or branch it pulls; old -> new when it changes."""
    git = s.get('git_source', {})
    if git.get('git_commit'):
        c = entry.get('changes', {}).get(COMMIT, {})
        old = c.get('old', c.get('remote'))
        if c.get('action', 'skip') != 'skip' and old:
            return f"commit {old[:7]}{tags(old)} -> {git['git_commit'][:7]}{tags(git['git_commit'])}"
        return f"commit {git['git_commit'][:7]}{tags(git['git_commit'])}"
    return f"branch {git.get('git_branch')}"


def protected(config, kind, key):
    """prevent_destroy when deploy refuses to delete it."""
    lifecycle = config.get('resources', {}).get(kind, {}).get(key, {}).get('lifecycle', {})
    return 'prevent_destroy' if lifecycle.get('prevent_destroy') else ''


def changes(entry, skip=()):
    """Fields a change would touch, skipping the ones plan marks unchanged
    and the ones in skip (already shown on the row)."""
    out = []
    for field, c in sorted(entry.get('changes', {}).items()):
        if c.get('action') == 'skip' or field in skip:
            continue
        old = c.get('old', c.get('remote'))
        new = c.get('new')
        out.append(f'{field}: {show(old)} -> {show(new)}')
    return out


def show(value):
    """A changed value; a full commit SHA is cut to 7 characters, as git log shows it."""
    if isinstance(value, str) and re.fullmatch(r'[0-9a-f]{40}', value):
        return json.dumps(value[:7])
    return json.dumps(value)


def tree(groups, width, verbose):
    """groups: [(root, root_note, [(leaf, action, note, key, fields)])], printed as
    root, then each leaf with its action and note; notes line up with root_note.
    width is the leaf column, shared by every tree so the actions line up."""
    note_at = 4 + width + 2 + 11
    for root, root_note, leaves in groups:
        print(f'  {root:<{note_at}}{root_note}'.rstrip())
        for i, (leaf, action, note, key, fields) in enumerate(leaves):
            last = i == len(leaves) - 1
            flag = '!' if action in ('delete', 'recreate') else ' '
            key = f'   ({key})' if verbose else ''
            print(f'  {LAST if last else MID} {leaf:<{width}} {flag}{action:<10} {note}{key}'.rstrip())
            for f in fields:
                print(f'  {"   " if last else BAR}     {f}')


def main():
    args = sys.argv[1:]
    verbose = any(a.lower() in VERBOSE for a in args)
    args = [a for a in args if a.lower() not in VERBOSE]
    if len(args) != 1 or args[0].startswith('-'):
        sys.exit(__doc__.strip().split('\n')[-1])
    target = args[0]
    config = cli('validate', '-t', target)
    entries = cli('plan', '-t', target)['plan']

    counts = {}
    jobs, catalogs, volumes = [], {}, {}
    for key, entry in entries.items():
        _, kind, name = key.split('.', 2)
        action = ACTIONS.get(entry['action'], entry['action'])
        counts[action] = counts.get(action, 0) + 1
        s = state(entry)
        if kind == 'jobs':
            jobs.append((s.get('name'), action, git_ref(s, entry), name, changes(entry, skip=(COMMIT,))))
        elif kind == 'schemas':
            catalogs.setdefault(s.get('catalog_name'), []).append(
                (s.get('name'), action, protected(config, kind, name), name, changes(entry)))
        else:
            volumes.setdefault(f"/Volumes/{s.get('catalog_name')}/{s.get('schema_name')}", []).append(
                (s.get('name'), action, protected(config, kind, name), name, changes(entry)))

    order = ['create', 'change', 'recreate', 'delete', 'unchanged']
    order += sorted(a for a in counts if a not in order)
    summary = ', '.join(f'{counts[a]} {a}' for a in order if a in counts)
    print(f'Plan for {target}: {summary or "nothing"}')

    # Volumes hang from their schema; schemas in layer order, raw first.
    layer = lambda p: (LAYERS.get(p.rsplit('/', 1)[-1], len(LAYERS)), p)
    sections = [
        ('Jobs', [(workspace(config), '', sorted(jobs))] if jobs else []),
        ('Schemas', [(c, '', sorted(catalogs[c], key=lambda l: layer(l[0])))
                     for c in sorted(catalogs)]),
        ('Volumes', [(p, '', sorted(volumes[p])) for p in sorted(volumes, key=layer)]),
    ]
    width = max((len(l[0]) for _, groups in sections for _, _, leaves in groups for l in leaves),
                default=0)
    for title, groups in sections:
        if groups:
            print(f'\n{title}')
            tree(groups, width, verbose)


if __name__ == '__main__':
    main()
