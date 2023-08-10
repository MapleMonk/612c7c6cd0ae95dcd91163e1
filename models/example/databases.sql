{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.myntra_catalog as SELECT a.sku_group, a.sku, b.myntra_status, SUM(a.INVENTORY_AVAILABLE) AS inventory_unicommerce, a.SKU_INVENTORY_GREATER_THAN_10_FLAG FROM ( SELECT sku_group, sku, SUM(units_on_hand) AS INVENTORY_AVAILABLE, SKU_INVENTORY_GREATER_THAN_10_FLAG FROM snitch_db.MAPLEMONK.INVENTORY_PLANNING_SUMMARY_SNITCH GROUP BY 1,2,4 ) a FULL OUTER JOIN ( SELECT REVERSE(SUBSTRING(REVERSE(VAN), CHARINDEX(\'-\', REVERSE(VAN)) + 1)) AS sku_group, \"Style Status Description\" AS myntra_status FROM snitch_db.MAPLEMONK.SNITCH_MYNTRA_LIVE GROUP BY 1, 2 ) b ON a.sku_group = b.sku_group GROUP BY 1,2,3,5",
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
                        