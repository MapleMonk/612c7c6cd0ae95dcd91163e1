{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table BSC_DB.MAPLEMONK.missing_utms as select distinct * from ( select distinct gokwik_utm_source Source, gokwik_utm_medium medium from BSC_DB.MAPLEMONK.Shopify_bombae_All_orders where gokwik_mapped_source = \'UNMAPPED\' union all select distinct gokwik_utm_source, gokwik_utm_medium from BSC_DB.MAPLEMONK.Shopify_All_orders where gokwik_mapped_source = \'UNMAPPED\' ) ; create or replace table BSC_DB.MAPLEMONK.missing_utms_GA as select distinct * from ( select distinct sessionsourcemedium from bsc_db.Maplemonk.BOMBAE_GA_SESSIONS_CONSOLIDATED where final_source = \'UNMAPPED\' union all select distinct sessionsourcemedium from bsc_db.Maplemonk.bsc_db_ga_SESSIONS_CONSOLIDATED where final_source = \'UNMAPPED\' ) ; create or replace table bsc_db.maplemonk.sku_mapping_missing as select distinct sku from BSC_DB.MAPLEMONK.BSC_DB_sales_consolidated a left join (select * from (select skucode, productname name, category, sub_category, brand, sku_type, row_number() over (partition by skucode order by 1) rw from BSC_DB.MAPLEMONK.sku_master) where rw = 1 ) p on lower(a.sku) = lower(p.skucode) where ORDER_TIMESTAMP::Date > \'2024-01-01\' and p.skucode is null ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from BSC_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            