{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table vahdam_db.maplemonk.vahdam_amazon_pnl_finevents_all_3p as select * from vahdam_db.maplemonk.vahdam_amazon_pnl_finevents_usa_3p union all select * from vahdam_db.maplemonk.vahdam_amazon_pnl_finevents_uk_3p union all select * from vahdam_db.maplemonk.vahdam_amazon_pnl_finevents_de_3p union all select * from vahdam_db.maplemonk.vahdam_amazon_pnl_finevents_uae_3p union all select * from vahdam_db.maplemonk.vahdam_amazon_pnl_finevents_aus_3p union all select * from vahdam_db.maplemonk.vahdam_amazon_pnl_finevents_ca_3p union all select * from vahdam_db.maplemonk.vahdam_amazon_pnl_finevents_in_3p;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from VAHDAM_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            