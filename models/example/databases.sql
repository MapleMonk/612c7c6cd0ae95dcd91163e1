{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.myntra_returns_processed as SELECT Style_id, seller_sku_code, MAX(return_reason) AS Highest_Reason, REVERSE(SUBSTRING(REVERSE(seller_sku_code), CHARINDEX(\'-\', REVERSE(seller_sku_code)) + 1)) AS sku_group FROM snitch_db.maplemonk.Myntra_Myntra_Returns GROUP BY Style_id,seller_sku_code,sku_group",
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
                        