{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.wtw_analysis as WITH live_date AS ( SELECT min(a.published_at::date) AS live_date, b.sku_group FROM snitch_db.maplemonk.shopifyindia_new_products A LEFT JOIN snitch_db.maplemonk.availability_master_v2 b ON a.id = b.id where a.published_at is not null and EXTRACT(DOW FROM published_at::date) = 4 and EXTRACT(HOUR FROM published_at) >= 17 and published_at::date >= \'2024-10-17\' group by 2 ), wtw_products as ( SELECT sku_group, live_date, case when live_date = \'2024-10-17\' then 1 else (datediff(day,\'2024-10-17\',live_date)/7)+1 end as wtw from live_date ), sales_data as ( select order_timestamp::date as order_date, sku_group, sum(gross_sales) as gross_sales, sum(quantity) as gross_quantity from snitch_db.maplemonk.fact_items_snitch where LOWER(IFNULL(discount_code, \'n\')) NOT LIKE \'%eco%\' AND LOWER(IFNULL(discount_code, \'n\')) NOT LIKE \'%influ%\' AND order_name NOT IN (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') AND tags NOT IN (\'FLITS_LOGICERP\') and EXTRACT(DOW FROM order_timestamp::date) = 4 and EXTRACT(HOUR FROM order_timestamp) >= 17 and order_timestamp::date >= \'2024-10-17\' group by 1,2 ), total_sales as ( select order_date, sum(gross_sales) as total_sales from sales_data group by 1 ), clicks as ( select ga_date, sku_group, sum(clicks) as clicks from snitch_db.maplemonk.clicks_itemid where type != \'Web\' and EXTRACT(DOW FROM ga_date) = 4 and ga_date >= \'2024-10-17\' group by 1,2 ), total_clicks as ( select ga_date, sum(clicks) as total_clicks from clicks group by 1 ) select a.sku_group, a.live_date, a.wtw, d.total_sales, e.total_clicks, coalesce(sum(b.gross_sales),0) as gross_sales, coalesce(sum(b.gross_quantity),0) as gross_quantity, coalesce(sum(c.clicks),0) as clicks from wtw_products a left join sales_data b on lower(trim(a.sku_group)) = lower(trim(b.sku_group)) and a.live_date = b.order_date left join clicks c on lower(trim(a.sku_group)) = lower(trim(c.sku_group)) and a.live_date = c.ga_date left join total_sales d on a.live_date = d.order_date left join total_clicks e on a.live_date = e.ga_date group by 1,2,3,4,5 ;",
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
            