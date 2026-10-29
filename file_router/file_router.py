import argparse
from pathlib import Path
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", default="utility")
args = parser.parse_args()

# Every utility catalog uses the same layout: raw.inbox and raw.files.
inbox = Path(f"/Volumes/{args.catalog}/raw/inbox")
files = Path(f"/Volumes/{args.catalog}/raw/files")
bad_files = inbox / "bad_files"
bad_files.mkdir(exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Each folder under files/ is a source. Longest first so a source like
# staging_monthly wins over a shorter one that is also a prefix of the name.
sources = sorted((p.name for p in files.iterdir() if p.is_dir()), key=len, reverse=True)

def match_source(name):
    for source in sources:
        if name.startswith(f"{source}_") and len(name) > len(source) + 1:
            return source, name[len(source) + 1:]
    return None, None

moved_good = []
moved_bad = []

for item in inbox.glob("*"):
    if item == bad_files:
        continue

    if item.is_dir():
        name = item.name
        source, foldername = match_source(name)
        if source is None:
            dbutils.fs.mv(str(item), str(bad_files / f"{name}_{timestamp}"), recurse=True)
            moved_bad.append(name)
            continue
        destination = files / source / foldername / timestamp
        destination.parent.mkdir(exist_ok=True)
        dbutils.fs.mv(str(item), str(destination), recurse=True)
        moved_good.append((name, str(destination)))

    else:
        stem, suffix = item.stem, item.suffix
        source, filename = match_source(stem)
        if source is None:
            dbutils.fs.mv(str(item), str(bad_files / f"{stem}_{timestamp}{suffix}"))
            moved_bad.append(item.name)
            continue
        destination = files / source / f"{filename}_{timestamp}{suffix}"
        dbutils.fs.mv(str(item), str(destination))
        moved_good.append((item.name, str(destination)))

print("==================================================")
print(f"  FILE ROUTER REPORT — {timestamp}")
print("==================================================")

print(f"\n👍👍👍 Successfully Routed: {len(moved_good)}")
for original, dest in moved_good:
    print(f"   {original} → {dest}")

print(f"\n👎👎👎 Moved to bad_files: {len(moved_bad)}")
for name in moved_bad:
    print(f"   {name}")

print("\n==================================================")
print(f"  Total processed: {len(moved_good) + len(moved_bad)}")
print("==================================================")
