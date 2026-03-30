create
or replace view edsm.silver.v_powerPlay as
select
  to_date(from_utc_timestamp(valid_from,'America/New_York')) as valid_from,
  to_date(from_utc_timestamp(valid_to,'America/New_York')) as valid_to,
  powerplay.* except (valid_from, valid_to)
from
  edsm.silver.powerplay powerplay;