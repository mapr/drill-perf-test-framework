-- tpch15 using 1395599672 as a seed to the RNG
-- use dfs.tmp; -- views can only be created in dfs schema


select
  s.s_suppkey,
  s.s_name,
  s.s_address,
  s.s_phone,
  r.total_revenue
from
  supplier s,
  revenue0 r
where
  s.s_suppkey = r.supplier_no
  and r.total_revenue = (
    select
      max(total_revenue)
    from
      revenue0
  )
order by
  s.s_suppkey;
  

