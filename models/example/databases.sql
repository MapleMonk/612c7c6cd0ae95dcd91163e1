{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.goods_to_despatch_main as ( SELECT CONCAT(SUBSTRING(DELIVEY_DATE_, 7, 4), \'-\', SUBSTRING(DELIVEY_DATE_, 4, 2), \'-\', SUBSTRING(DELIVEY_DATE_, 1, 2)) AS converted_date, SKU AS sku_group, descipton, class, new_style, Online_qty AS inward_inventory FROM snitch_db.maplemonk.gs_goods_to_despatch_main );",
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
                        