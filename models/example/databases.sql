{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.sku_group_tags as select REVERSE(SUBSTRING(REVERSE(PARSE_JSON(variants)[0]:sku), CHARINDEX(\'-\', REVERSE(PARSE_JSON(variants)[0]:sku)) + 1)) AS sku_group, TAGS FROM snitch_db.maplemonk.SHOPIFYINDIA_products",
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
                        