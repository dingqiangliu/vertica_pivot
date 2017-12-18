 <meta charset='utf-8'>

Pivot/Unpivot for Vertica, convert columns to rows, or rows to columns
==========
This is a Vertica User Defined Functions (UDF) for string strcat function, just like Oracle's.

Syntax:
----------

1. pivot ( measureName, measureValue  using parameters columnsFilter=':columnsFilter' [, separator=':separator'] ) over(...)

Parameters:

 * measureName: string.
 * measureValue: int/float/numeric.
 * columnsFilter: keeping measure names.
 * separator: separator string for multiple mearue name, default value is ','.
 * (return): convert each different values of 1st parameter as columns, and sum measureValues as value. 

&nbsp;

2. unpivot ( measureValue [, measureValue ...]  using parameters measureNames=':columnsFilter' [, separator=':separator'] ) over(...) [as (measureName, measureValue)]

Parameters:

 * measureValue: int/float/numeric.
 * measureNames: measure names, one meaure name for a mesure value column .
 * separator: separator string for concatenating, default value is ','.
 * (return): convert columns to rows, all measureValue show in same column "measureValue" but with different "measureName". 

Examples:
----------

- pivot with standard SQL:

<code><pre>
    select call_center_key, 
      sum(decode(d.date, '2003-01-01'::date, sales_dollar_amount)) as "2003-01-01", 
      sum(decode(d.date, '2003-01-02'::date, sales_dollar_amount)) as "2003-01-02", 
      sum(decode(d.date, '2003-01-03'::date, sales_dollar_amount)) as "2003-01-03"
    from online_sales.online_sales_fact f 
      inner join date_dimension d on f.sale_date_key = d.date_key 
    where d.date >= '2003-01-01' and d.date <= '2003-01-03' 
      and call_center_key >= 1 and call_center_key <= 3 
    group by call_center_key 
    order by call_center_key 
    ;
     call_center_key | 2003-01-01 | 2003-01-02 | 2003-01-03 
    -----------------+------------+------------+------------
                   1 |       1123 |       6014 |        231
                   2 |       3053 |       6646 |       3320
                   3 |       2732 |       6476 |       6030
    (3 rows)

</code></pre>

- pivot with UDTF:

<code><pre>
    select call_center_key, 
      pivot(d.date::varchar, sales_dollar_amount using parameters columnsFilter = '2003-01-01,2003-01-02', separator = ',') over(partition by call_center_key)
    from online_sales.online_sales_fact f 
      inner join date_dimension d on f.sale_date_key = d.date_key 
    where d.date >= '2003-01-01' and d.date <= '2003-01-03' 
      and call_center_key >= 1 and call_center_key <= 3 
    order by call_center_key 
    ;
     call_center_key | 2003-01-01 | 2003-01-02 
    -----------------+------------+------------
                   1 |       1123 |       6014
                   2 |       3053 |       6646
                   3 |       2732 |       6476
    (3 rows)
</code></pre>


- unpivot with standard SQL:

<code><pre>
      select call_center_key, 'sales_dollar_amount' as measureType, sum(sales_dollar_amount) as measureValue 
      from online_sales.online_sales_fact f 
      where call_center_key >= 1 and call_center_key <= 3 
      group by call_center_key 
      union all
      select call_center_key, 'ship_dollar_amount' as measureType, sum(ship_dollar_amount) as measureValue 
      from online_sales.online_sales_fact f 
      where call_center_key >= 1 and call_center_key <= 3 
      group by call_center_key 
      union all
      select call_center_key, 'net_dollar_amount' as measureType, sum(net_dollar_amount) as measureValue 
      from online_sales.online_sales_fact f 
      where call_center_key >= 1 and call_center_key <= 3 
      group by call_center_key 
    ) t
    order by call_center_key, measureType 
    ;
     call_center_key |     measureNames     | measureValue 
    -----------------+---------------------+--------------
                   1 | net_dollar_amount   |      7010477
                   1 | sales_dollar_amount |      6786660
                   1 | ship_dollar_amount  |       223817
                   2 | net_dollar_amount   |      7005520
                   2 | sales_dollar_amount |      6781826
                   2 | ship_dollar_amount  |       223694
                   3 | net_dollar_amount   |      7170745
                   3 | sales_dollar_amount |      6943208
                   3 | ship_dollar_amount  |       227537
    (9 rows)
</code></pre>

- unpivot with UDTF:

<code><pre>
    select call_center_key, 
      unpivot(sum(sales_dollar_amount), sum(ship_dollar_amount), sum(net_dollar_amount) using parameters measureNames='sales_dollar_amount,ship_dollar_amount,net_dollar_amount', separator =',') over(partition by call_center_key) as (measureType, measureValue)
    from online_sales.online_sales_fact f 
    where call_center_key >= 1 and call_center_key <= 3 
    group by call_center_key 
    order by call_center_key, measureType 
    ;
     call_center_key |     measureName     | measureValue 
    -----------------+---------------------+--------------
                   1 | net_dollar_amount   |      7010477
                   1 | sales_dollar_amount |      6786660
                   1 | ship_dollar_amount  |       223817
                   2 | net_dollar_amount   |      7005520
                   2 | sales_dollar_amount |      6781826
                   2 | ship_dollar_amount  |       223694
                   3 | net_dollar_amount   |      7170745
                   3 | sales_dollar_amount |      6943208
                   3 | ship_dollar_amount  |       227537
    (9 rows)
</code></pre>



Install, test and uninstall:
----------
Befoe build and install, g++ should be available(yum -y groupinstall "Development tools" && yum -y groupinstall "Additional Development" can help on this).

 * Build: make
 * Install: make install
 * Test: make run
 * Uninstall make uninstall

</body> </html>



