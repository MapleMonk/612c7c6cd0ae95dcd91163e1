{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.retention_snitch as select c.month, c.year, LTM_all_cust, LTM_new_cust, TM_new_cust, TM_old_cust from( select month(date_start) month , year(date_Start) year , count(distinct b.customer_id ) LTM_all_cust from (select distinct date_trunc(\'month\',order_timestamp::date) date_start from snitch_db.maplemonk.fact_items_snitch) a left join snitch_db.maplemonk.fact_items_snitch b on b.order_timestamp::date < a.date_start and b.order_timestamp::date > a.date_start - 365 where lower(b.order_status) <> \'cancelled\' group by 1,2 order by 2,1 desc )c left join (select month(order_timestamp::date) month , year(order_timestamp::date) year , count(distinct case when new_customer_flag = \'New\' then customer_id end) TM_new_cust , count(distinct case when new_customer_flag <> \'New\' then customer_id end) TM_old_cust from snitch_db.maplemonk.fact_items_snitch where lower(order_status) <> \'cancelled\' group by 1,2 order by 2,1 desc) d on c.month=d.month and c.year=d.year left join ( select month(date_start) month , year(date_Start) year , count(distinct b.customer_id ) LTM_new_cust from (select distinct date_trunc(\'month\',order_timestamp::date) date_start from snitch_db.maplemonk.fact_items_snitch) a left join snitch_db.maplemonk.fact_items_snitch b on b.order_timestamp::date < a.date_start and b.order_timestamp::date > a.date_start - 365 where customer_flag = \'New\' and lower(order_status) <> \'cancelled\' group by 1,2 order by 2,1 desc )e on e.month = c.month and e.year = c.year",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from snitch_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        