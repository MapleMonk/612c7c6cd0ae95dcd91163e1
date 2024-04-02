{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.bucket_class_category_clicks_impressions AS WITH class_category_data AS ( SELECT order_date, sku_group, sku_class, category, SUM(suborder_quantity) as gross_sales FROM snitch_db.maplemonk.unicommerce_fact_items_snitch WHERE order_date >= \'2024-03-01\' GROUP BY 1, 2, 3, 4 ), impressions_click_data AS ( SELECT GA_DATE, sku_group, SUM(impressions) AS impressions, SUM(clicks) AS clicks FROM snitch_db.maplemonk.final_ga_clicks_impressions_by_itemid WHERE GA_DATE >= \'2024-03-01\' GROUP BY 1, 2 ) SELECT cnc.order_date, cnc.sku_group, cnc.sku_class, cnc.category, ic.impressions, ic.clicks, cnc.gross_sales FROM class_category_data cnc LEFT JOIN impressions_click_data ic ON cnc.sku_group = ic.sku_group AND cnc.order_date = ic.GA_DATE;",
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
                        