create
or replace view edsm.silver.v_powerPlay as
select
  to_date(from_utc_timestamp(valid_from,'America/New_York')) as valid_from,
  to_date(from_utc_timestamp(valid_to,'America/New_York')) as valid_to,
  date,
  allegiance,
  coords,
  government,
  id,
  id64,
  name,
  power,
  powerState,
  state
from
  edsm.silver.powerplay powerplay;