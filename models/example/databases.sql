{{ config(
            materialized='table',
                post_hook={
                    "sql": "SELECT * FROM zeproc_db.maplemonk.ZEPROC_MAGENTO_AMASTY_AMSHOPBY_OPTION_SETTING WHERE store_id=3;",
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
            