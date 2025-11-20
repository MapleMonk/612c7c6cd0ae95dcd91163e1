{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table zeproc_db.maplemonk.gsc_and_sales_consolidated as select * from zeproc_db.maplemonk.zeproc_gsc_search_analytics_all_fields;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from ZEPROC_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            