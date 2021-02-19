create or replace temporary view stays_expedia as
select ex.hotel_id, ex.srch_children_cnt, datediff(ex.srch_co, ex.srch_ci) as stay
from enriched_expedia ex