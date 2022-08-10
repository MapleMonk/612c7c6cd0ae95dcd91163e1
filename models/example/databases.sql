{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table demo_db.MAPLEMONK.test_table as select b.product_description,* from DEMO_DB.MAPLEMONK.SHOPIFY_DUMMY_DATA a left join DEMO_DB.MAPLEMONK.PRODUCT_MAPPING b on a.product_name = b.product_name",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from DEMO_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        