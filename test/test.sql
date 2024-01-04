/*****************************
 *
 * Pivot User Defined Functions
 *
 * Copyright DingQiang Liu(dingqiangliu@gmail.com), 2012 - 2017
 */


\echo
\echo '******************************'
\echo 'pivot with standard SQL ...'

-- SUM
select call_center_key, 
  sum(decode(d.date, '2003-01-01'::date, sales_dollar_amount)) as "2003-01-01", 
  sum(decode(d.date, '2003-01-02'::date, sales_dollar_amount)) as "2003-01-02", 
  sum(decode(d.date, '2003-01-03'::date, sales_dollar_amount)) as "2003-01-03"
from online_sales.online_sales_fact f 
  inner join date_dimension d on f.sale_date_key = d.date_key 
where d.date >= '2003-01-01' and d.date <= '2003-01-03' 
  and call_center_key >= 1 and call_center_key <= 3 
group by call_center_key 
order by 1 
;

-- LAST
select call_center_key, max("2003-01-01") as "2003-01-01", max("2003-01-02") as "2003-01-02", max("2003-01-03") as "2003-01-02"
from (
select call_center_key, 
  decode(d.date, '2003-01-01'::date, sales_dollar_amount) as "2003-01-01", 
  decode(d.date, '2003-01-02'::date, sales_dollar_amount) as "2003-01-02", 
  decode(d.date, '2003-01-03'::date, sales_dollar_amount) as "2003-01-03",
  row_number() over(partition by call_center_key, d.date order by pos_transaction_number desc)as rn
from online_sales.online_sales_fact f 
  inner join date_dimension d on f.sale_date_key = d.date_key 
where d.date >= '2003-01-01' and d.date <= '2003-01-03' 
  and call_center_key >= 1 and call_center_key <= 3
  ) t
where t.rn = 1
group by call_center_key
order by call_center_key 
;

\echo
\echo '******************************'
\echo 'pivot with UDTF ...'
-- SUM
select call_center_key, 
  pivot(d.date::varchar, sales_dollar_amount::int using parameters columnsFilter = '2003-01-01,2003-01-02,2003-01-03') over(partition by call_center_key) 
from online_sales.online_sales_fact f 
  inner join date_dimension d on f.sale_date_key = d.date_key 
where d.date >= '2003-01-01' and d.date <= '2003-01-03' 
  and call_center_key >= 1 and call_center_key <= 3 
order by 1 
;

-- FIRST
select call_center_key, 
  pivot(d.date::varchar, sales_dollar_amount using parameters columnsFilter = '2003-01-01,2003-01-02,2003-01-03', separator = ',', method = 'FIRST') over(partition by call_center_key order by pos_transaction_number desc)
from online_sales.online_sales_fact f 
  inner join date_dimension d on f.sale_date_key = d.date_key 
where d.date >= '2003-01-01' and d.date <= '2003-01-03' 
  and call_center_key >= 1 and call_center_key <= 3 
order by call_center_key 
;

-- LAST
select call_center_key, 
  pivot(d.date::varchar, sales_dollar_amount using parameters columnsFilter = '2003-01-01,2003-01-02,2003-01-03', separator = ',', method = 'LAST') over(partition by call_center_key order by pos_transaction_number)
from online_sales.online_sales_fact f 
  inner join date_dimension d on f.sale_date_key = d.date_key 
where d.date >= '2003-01-01' and d.date <= '2003-01-03' 
  and call_center_key >= 1 and call_center_key <= 3 
order by call_center_key 
;

-- multiple measures
select call_center_key, 
  pivot(d.date::varchar, sales_quantity::int, sales_dollar_amount::float using parameters columnsFilter = '2003-01-01,2003-01-02', separator = ',') over(partition by call_center_key)
from online_sales.online_sales_fact f 
  inner join date_dimension d on f.sale_date_key = d.date_key 
where d.date >= '2003-01-01' and d.date <= '2003-01-03' 
  and call_center_key >= 1 and call_center_key <= 3 
order by 1 
;

-- less columns required than data
select call_center_key, 
  pivot(d.date::varchar, sales_dollar_amount::int using parameters columnsFilter = '2003-01-01,2003-01-02,2003-01-03') over(partition by call_center_key) 
from online_sales.online_sales_fact f 
  inner join date_dimension d on f.sale_date_key = d.date_key 
where d.date >= '2003-01-01' and d.date <= '2003-01-03' 
  and call_center_key >= 1 and call_center_key <= 3 
order by 1 
;

-- more columns required than data
select call_center_key, 
  pivot(d.date::varchar, sales_dollar_amount::numeric using parameters columnsFilter = '2003-01-01|2003-01-02|2003-01-03|2003-01-04', separator = '|') over(partition by call_center_key)
from online_sales.online_sales_fact f 
  inner join date_dimension d on f.sale_date_key = d.date_key 
where d.date >= '2003-01-01' and d.date <= '2003-01-03' 
  and call_center_key >= 1 and call_center_key <= 3 
order by 1 
;


\echo
\echo '******************************'
\echo 'unpivot with standard SQL ...'
select * from (
  select call_center_key, 'sales_dollar_amount' as measureName, sum(sales_dollar_amount) as measureValue 
  from online_sales.online_sales_fact f 
  where call_center_key >= 1 and call_center_key <= 3 
  group by call_center_key 
  union all
  select call_center_key, 'ship_dollar_amount' as measureName, sum(ship_dollar_amount) as measureValue 
  from online_sales.online_sales_fact f 
  where call_center_key >= 1 and call_center_key <= 3 
  group by call_center_key 
  union all
  select call_center_key, 'net_dollar_amount' as measureName, sum(net_dollar_amount) as measureValue 
  from online_sales.online_sales_fact f 
  where call_center_key >= 1 and call_center_key <= 3 
  group by call_center_key 
) t
order by 1, 2 
;


\echo
\echo '******************************'
\echo 'unpivot with UDTF ...'
select call_center_key, 
  unpivot(sum(sales_dollar_amount::int), sum(ship_dollar_amount::int), sum(net_dollar_amount::int) using parameters measureNames='sales_dollar_amount,ship_dollar_amount,net_dollar_amount') over(partition by call_center_key)
from online_sales.online_sales_fact f 
where call_center_key >= 1 and call_center_key <= 3 
group by call_center_key 
order by 1, 2
;

select call_center_key, 
  unpivot(sum(sales_dollar_amount::float), sum(ship_dollar_amount::float), sum(net_dollar_amount::float) using parameters measureNames='sales_dollar_amount,ship_dollar_amount,net_dollar_amount', separator =',') over(partition by call_center_key) as (measureName, measureValue)
from online_sales.online_sales_fact f 
where call_center_key >= 1 and call_center_key <= 3 
group by call_center_key 
order by 1, 2
;

select call_center_key, 
  unpivot(sum(sales_dollar_amount::numeric), sum(ship_dollar_amount::numeric), sum(net_dollar_amount::numeric) using parameters measureNames='sales_dollar_amount|ship_dollar_amount|net_dollar_amount', separator ='|') over(partition by call_center_key) as (measureName, measureValue)
from online_sales.online_sales_fact f 
where call_center_key >= 1 and call_center_key <= 3 
group by call_center_key 
order by 1, 2
;

-- for date type
select key, unpivot(SJ1, SJ2 using parameters measureNames='SJ1|SJ2', separator ='|') over(partition by key)
from (select 1 key, '2024-01-01'::date SJ1, '2024-01-02'::date SJ2 from dual) t
order by 1, 2;
