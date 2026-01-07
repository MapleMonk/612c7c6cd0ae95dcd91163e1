{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.product_tracking_collated_jan26 as with product_tracking as ( select a.*, regexp_replace(a.subvention, \'^R\', \'\') as skugroup_norm, date_trunc(\'day\', try_to_date(a.date_issued, \'DD/MM/YYYY\')) as issued_date, date_trunc(\'day\', try_to_date(a.revised_delivery_date, \'DD/MM/YYYY\')) as new_delivery_date, date_trunc(\'day\', try_to_date(a.expected_delivery_date, \'DD/MM/YYYY\')) as exp_delivery_date from snitch_db.maplemonk.gs_product_tracking_new_main a where a.sku_status not like \'%Cancel%\' ), sku_list as ( select distinct skugroup_norm from product_tracking ), putaway_base as ( select regexp_replace( case when position(\'-\' in \"Item Type skuCode\") > 0 then left( \"Item Type skuCode\", length(\"Item Type skuCode\") - length(reverse(substring(reverse(\"Item Type skuCode\"), 1, position(\'-\', reverse(\"Item Type skuCode\")) - 1))) - 1 ) else \"Item Type skuCode\" end, \'^R\', \'\' ) as skugroup_norm, date_trunc(\'day\', putaway_completed_date) as putaway_completed_date, putaway_completed_quantity from snitch_db.maplemonk.putaway_tracking ), putaway_daily as ( select p.skugroup_norm, p.putaway_completed_date, sum(p.putaway_completed_quantity) as putaway_qty from putaway_base p join sku_list s on s.skugroup_norm = p.skugroup_norm group by 1,2 ), putaway_latest as ( select skugroup_norm, putaway_completed_date, putaway_qty from putaway_daily qualify row_number() over ( partition by skugroup_norm order by putaway_completed_date desc ) = 1 ), main_data as ( select a.*, b.putaway_completed_date, b.putaway_qty, case when upper(a.sku_status_) = \'DELIVERED\' then datediff(\'day\', a.issued_date, b.putaway_completed_date) when upper(a.sku_status_) in (\'ACTIVE\', \'FABRIC PLACED\', \'RTS\') then datediff(\'day\', a.issued_date, greatest(COALESCE(a.new_delivery_date, \'1900-01-01\'::DATE), COALESCE(a.exp_delivery_date, \'1900-01-01\'::DATE))) else null end as lead_time_days from product_tracking a left join putaway_latest b on a.skugroup_norm = b.skugroup_norm) select SUBVENTION, SKU_STATUS_, ISSUED_DATE, EXP_DELIVERY_DATE, NEW_DELIVERY_DATE, PUTAWAY_COMPLETED_DATE, PUTAWAY_QTY, LEAD_TIME_DAYS, MRP, AGEING, SEASON, B2B_QTY, CUT_QTY, FI_DATE, RTS_QTY, REMARKS, CATEGORY, PROJ_QTY, FI_STATUS, LEAD_TIME, ORDER_TYPE, FABRIC_CODE, FACTORY_NAME, INCHARG_NAME, CUTTING_RATIO, FABRIC_METERS, FABRIC_ORIGIN, FABRIC_VENDOR, SKUGROUP_NORM from main_data ;",
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
            