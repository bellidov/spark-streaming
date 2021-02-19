create or replace temporary view hotels_data as
select
get_json_object(cast(value as string),'$.id') as id,
get_json_object(cast(value as string),'$.name') as name,
get_json_object(cast(value as string),'$.country') as country,
get_json_object(cast(value as string),'$.city') as city,
get_json_object(cast(value as string),'$.address') as address,
get_json_object(cast(value as string),'$.avg_tmpr_c') as avg_tmpr,
get_json_object(cast(value as string),'$.wthr_date') as wthr_date
from incoming_hotels