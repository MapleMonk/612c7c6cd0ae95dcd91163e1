{{ config(
            materialized='table',
                post_hook={
                    "sql": "SELECT * FROM `MAPLEMONK.Unicommerence_Freakins_get_grn_report_new`",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            