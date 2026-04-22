create or replace view osv.silver.v_pypi_history as
select
  to_date(from_utc_timestamp(valid_from, 'America/New_York')) as valid_from,
  to_date(from_utc_timestamp(valid_to,   'America/New_York')) as valid_to,
  current_flag,
  id,
  schema_version,
  summary,
  details,
  published,
  modified,
  withdrawn,
  upstream,
  references,
  affected,
  aliases,
  severity,
  credits,
  row_hash,
  ingest_time
from
  osv.silver.pypi_history;
