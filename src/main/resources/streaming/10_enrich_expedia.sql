create or replace temporary view enriched_expedia as
select ex.hotel_id, ex.srch_children_cnt, ex.srch_co, ex.srch_ci, hw.avg_tmpr
from incoming_expedia_2016 ex left join hotels_data hw
on hw.id = ex.hotel_id and hw.wthr_date = ex.srch_ci
where hw.avg_tmpr > 0
union
select ex.hotel_id, ex.srch_children_cnt, ex.srch_co, ex.srch_ci, hw.avg_tmpr
from incoming_expedia_2017 ex left join hotels_data hw
on hw.id = ex.hotel_id and hw.wthr_date = ex.srch_ci
where hw.avg_tmpr > 0
