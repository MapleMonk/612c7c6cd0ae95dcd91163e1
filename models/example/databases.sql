{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.product_tracking_collated_jan26 as with product_tracking as ( select a.*, date_trunc(\'day\', try_to_date(a.date_issued, \'DD/MM/YYYY\')) as issued_date, date_trunc(\'day\', try_to_date(a.revised_delivery_date, \'DD/MM/YYYY\')) as new_delivery_date, date_trunc(\'day\', try_to_date(a.expected_delivery_date, \'DD/MM/YYYY\'))as exp_delivery_date from snitch_db.maplemonk.gs_product_tracking_new_main a ), putaway_report as ( select skugroup, date_trunc(\'day\', putaway_completed_date) as putaway_completed_date, sum(putaway_completed_quantity) as putaway_qty from snitch_db.maplemonk.putaway_tracking group by 1,2) select a.*, b.putaway_completed_date, b.putaway_qty, case when upper(a.sku_status_) = \'DELIVERED\' then datediff(\'day\',a.issued_date,b.putaway_completed_date) when upper(a.sku_status_) in (\'ACTIVE\', \'FABRIC PLACED\', \'RTS\') then datediff(\'day\',a.issued_date, greatest(a.new_delivery_date, a.exp_delivery_date)) else null end as lead_time_days from product_tracking a left join putaway_report b on a.subvention = b.skugroup",
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
            