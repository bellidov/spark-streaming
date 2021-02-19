create or replace temporary view preferences_expedia as
with preferences as (
select
  ex.hotel_id,
  case
    when ex.stay is null or ex.stay > 30 or ex.stay <= 0 then 'Erroneous data'
    when ex.stay = 1 then 'Short day'
    when ex.stay > 1 and ex.stay <= 7 then 'Standart stay'
    when ex.stay > 7 and ex.stay <= 14 then 'Standart extended stay'
    when ex.stay > 14 and ex.stay <= 28 then 'Long stay'
    else 'NA'
  end as preference,
  case
    when ex.srch_children_cnt > 0 then true
    else false
  end as withChildren
from stays_expedia ex
)
select * from preferences