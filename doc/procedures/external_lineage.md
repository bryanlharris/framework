# Add External Lineage

Unity Catalog draws lineage from bronze onward by itself. It does not
know where the raw files came from. External lineage adds that first
box: an external metadata object for the source (its URL), and an
arrow from it to the bronze table.

The bodies for EDSM are in **doc/examples/lineage/**: one
**\<catalog>.\<table>.metadata.json** (the source) and one
**\<catalog>.\<table>.lineage.json** (the arrow) per table. Copy them
for a new source.

Notes:

- Needs CREATE EXTERNAL METADATA on the metastore, from the metastore
admin. Free doesn't have it.
- Objects are metastore-wide, so one object serves every workspace.
Create it once; add one arrow per environment's bronze table
(dev_edsm..., staging_edsm..., edsm...).
- The web form can't take JSON, so use the CLI from the repo folder.

1. Create the source object:

```
databricks external-metadata create-external-metadata --json @doc/examples/lineage/edsm.powerPlay.metadata.json -p <profile>
```

2. Create the arrow to the bronze table:

```
databricks external-lineage create-external-lineage-relationship --json @doc/examples/lineage/edsm.powerPlay.lineage.json -p <profile>
```

3. Check: in Catalog Explorer, open the bronze or silver table and
look at the Lineage tab. The source shows upstream.
