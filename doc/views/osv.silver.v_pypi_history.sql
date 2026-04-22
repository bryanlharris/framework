create or replace view osv.silver.v_pypi_history as
select
  to_date(from_utc_timestamp(valid_from, 'America/New_York')) as valid_from,
  to_date(from_utc_timestamp(valid_to,   'America/New_York')) as valid_to,
  current_flag,
  id,
  summary,
  details,
  schema_version,
  published,
  modified,
  withdrawn
from
  osv.silver.pypi_history;
