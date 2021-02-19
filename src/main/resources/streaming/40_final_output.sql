create or replace temporary view preferences as
with counter_preferences as (
select
  ex.hotel_id,
  count(
    case
      when ex.preference like 'Erroneous data' then 'count'
      else null
    end) as error_ct,
  count(
    case
      when ex.preference like 'Short day' then 'count'
      else null
    end) as short_ct,
  count(
    case
      when ex.preference like 'Standart stay' then 'count'
      else null
    end) as standart_ct,
  count(
    case
      when ex.preference like 'Standart extended stay' then 'count'
      else null
    end) as st_ext_ct,
  count(
    case
      when ex.preference like 'Long stay' then 'count'
      else null
    end) as long_ct
from preferences_expedia ex
group by ex.hotel_id
)
select
  hotel_id,
  case
   when error_ct = greatest(error_ct, short_ct, standart_ct, st_ext_ct, long_ct) then 'Erroneous data'
   when short_ct = greatest(error_ct, short_ct, standart_ct, st_ext_ct, long_ct) then 'Short day'
   when standart_ct = greatest(error_ct, short_ct, standart_ct, st_ext_ct, long_ct) then 'Standart stay'
   when st_ext_ct = greatest(error_ct, short_ct, standart_ct, st_ext_ct, long_ct) then 'Standart extended stay'
   when long_ct = greatest(error_ct, short_ct, standart_ct, st_ext_ct, long_ct) then 'Long stay'
  end as preference
from counter_preferences
