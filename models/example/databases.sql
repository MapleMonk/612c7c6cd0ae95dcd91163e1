{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.offline_repeat_new_sales as with main as ( select marketplace_mapped, \'+91\' || RIGHT(REGEXP_REPLACE(phone, \'[^0-9]\', \'\'), 10) AS phone_no, order_date, sum(selling_price) as sales from snitch_db.maplemonk.store_fact_items_offline WHERE LOWER(marketplace_mapped) NOT LIKE \'%wh%\' group by 1,2,3 ), acq as ( select phone_no, store, first_order_date from snitch_db.maplemonk.customer_acq_date_channel ), store_acq as ( select phone_no, min(order_date) as store_acq_date from main group by 1 ), store_wise_acq as ( select \'+91\' || RIGHT(REGEXP_REPLACE(phone, \'[^0-9]\', \'\'), 10) AS phone_no, marketplace_mapped, min(order_date) as storewise_acq_date from snitch_db.maplemonk.store_fact_items_offline WHERE LOWER(marketplace_mapped) NOT LIKE \'%wh%\' group by 1,2 ), pre_check as ( select a.*, b.store, b.first_order_date, c.store_acq_date, case when order_date <= first_order_date then \'new\' else \'repeat\' end as overall_repeat, case when order_date <= store_acq_date then \'new\' else \'repeat\' end as offline_repeat, case when order_date <= storewise_acq_date then \'new\' else \'repeat\' end as offline_store_repeat from main a left join acq b on a.phone_no = b.phone_no left join store_acq c on a.phone_no = c.phone_no left join store_wise_acq d on a.phone_no = d.phone_no and a.marketplace_mapped = d.marketplace_mapped ) select marketplace_mapped, order_date, overall_repeat, offline_repeat, offline_store_repeat, sum(sales) as sales from pre_check group by 1,2,3,4,5 ;",
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
            