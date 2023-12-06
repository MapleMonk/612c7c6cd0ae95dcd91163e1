{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table elcinco_db.maplemonk.elcinco_db_inventory_summary as with inventory as ( select facility,skucode, sum(inventory) inventory from elcinco_db.maplemonk.elcinco_unicommerce_get_inventory_snapshot group by 1,2 union all select \'Myntra SJIT\' as facility ,b.van skucode, sum(\"Sellable Inventory Count\"::float) inventory from ( select * from elcinco_db.maplemonk.myntra_sjit_inventory where _AB_SOURCE_FILE_LAST_MODIFIED = (select max(_AB_SOURCE_FILE_LAST_MODIFIED) from elcinco_db.maplemonk.myntra_sjit_inventory) ) a left join ( select sku, van from ( select \"SKU Code\" sku, van, row_number() over (partition by \"SKU Code\" order by 1) rw from elcinco_db.maplemonk.myntra_sku_mapping )where rw = 1 ) b on a.\"SKU Code\" = b.sku group by 1,2 ) , sales as ( select case when warehouse in (\'28\',\'36\',\'81\',\'15774\',\'309\') then \'Myntra SJIT\' else warehouse end as facility, sku skucode, sum(case when datediff(day, order_date::date, getdate()::date) <=90 then quantity end) as last_90_day_sales, sum(case when datediff(day, order_date::date, getdate()::date) <=30 then quantity end) as last_30_day_sales, sum(case when datediff(day, order_date::date, getdate()::date) <=14 then quantity end) as last_14_day_sales, sum(case when datediff(day, order_date::date, getdate()::date) <=7 then quantity end) as last_7_day_sales from elcinco_db.MAPLEMONK.elcinco_db_sales_consolidated group by 1,2 ) , weekly_max as ( select facility, skucode, week, weekly_sales weekly_max from ( select *, row_number() over (partition by facility, skucode order by weekly_sales desc) rw from ( select case when warehouse in (\'28\',\'36\',\'81\',\'15774\',\'309\') then \'Myntra SJIT\' else warehouse end as facility, sku skucode, date_trunc(week,ordeR_DAte::Date) week, ifnull(sum(quantity),0) weekly_sales from elcinco_db.MAPLEMONK.elcinco_db_sales_consolidated where datediff(day, order_date::date, getdate()::date) <=60 group by 1,2,3 ))where rw = 1 ) , weekly_min as ( select facility, skucode, week, weekly_sales weekly_min from ( select *, row_number() over (partition by facility, skucode order by weekly_sales asc) rw from ( select case when warehouse in (\'28\',\'36\',\'81\',\'15774\',\'309\') then \'Myntra SJIT\' else warehouse end as facility, sku skucode, date_trunc(week,ordeR_DAte::Date) week, ifnull(sum(quantity),0) weekly_sales from elcinco_db.MAPLEMONK.elcinco_db_sales_consolidated where datediff(day, order_date::date, getdate()::date) <=60 group by 1,2,3 ))where rw = 1 ) select i.facility, i.skucode, i.inventory, last_90_day_sales, last_30_day_sales, last_14_day_sales, last_7_day_sales, weekly_max, weekly_min from inventory i left join sales s on s.skucode = i.skucode and lower(s.facility) = lower(i.facility) left join weekly_max wmax on i.skucode = wmax.skucode and lower(i.facility) = lower(wmax.facility) left join weekly_min wmin on i.skucode = wmin.skucode and lower(i.facility) = lower(wmin.facility) ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from elcinco_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        