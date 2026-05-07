{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.geowis_adset_spend_sale_data as WITH meta_cities AS ( SELECT LOWER(SPLIT_PART(adset_name, \'_\', 1)) AS city, date AS activity_date, SUM(spend) AS spend, SUM(total_purchase_offline) AS total_purchase_offline, SUM(total_purchase) as total_purchase FROM snitch_db.maplemonk.meta_scorecard WHERE type = \'Geo_WIS\' GROUP BY LOWER(SPLIT_PART(adset_name, \'_\', 1)), date ), branch_city_map AS ( SELECT DISTINCT branch_code, LOWER(city) AS city FROM snitch_db.maplemonk.offline_master ), sales_by_city_date AS ( SELECT bcm.city, sfi.order_date, SUM(sfi.selling_price) AS sales_on_date FROM snitch_db.maplemonk.store_fact_items_offline sfi INNER JOIN branch_city_map bcm ON bcm.branch_code = sfi.branch_code GROUP BY bcm.city, sfi.order_date ) SELECT mc.city, mc.activity_date, (mc.spend::int) AS spends, (mc.total_purchase_offline::int) AS total_purchase_offline, (mc.total_purchase::int) AS total_purchase, (s.sales_on_date::int) AS sale_on_date FROM meta_cities mc LEFT JOIN sales_by_city_date s ON s.city = mc.city AND s.order_date = mc.activity_date ORDER BY mc.city, mc.activity_date;",
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
            