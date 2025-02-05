{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.SFUCCHECK AS SELECT sf.*, uc.order_name as uc_statusas FROM snitch_db.maplemonk.fact_items_snitch sf LEFT JOIN snitch_db.maplemonk.warehouse_sla_performance uc ON sf.order_name = uc.order_name WHERE LOWER(COALESCE(tags, \'\')) NOT LIKE \'%eco%\' AND LOWER(COALESCE(tags, \'\')) NOT LIKE \'%rms%\' AND LOWER(COALESCE(tags, \'\')) NOT LIKE \'%exch%\' AND sf.order_timestamp >= \'2025-01-01\' and city_mapped != \'TEST\' AND uc.order_name is null and sf.order_status not in (\'CANCELLED\') order by order_timestamp asc ; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.UCLOGICCHECK AS select uc.order_name,uc.dispatched_timestamp,uc.marketplace_mapped, le.order_no from snitch_db.maplemonk.warehouse_sla_performance uc left join snitch_db.maplemonk.logicerpnew_get_sale_invoice le on uc.order_name = le.order_no where uc.marketplace_mapped not in (\'STORE\',\'CUSTOM\') and uc.dispatched_timestamp <= current_date - 1 and uc.order_date >= \'2025-02-01\'",
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
            