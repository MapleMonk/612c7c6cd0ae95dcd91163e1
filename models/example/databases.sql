{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.black_friday_analytics as with shopify_pre as ( select *, case when LOWER(IFNULL(a.discount_code, \'n\')) LIKE \'%eco%\' then 1 when LOWER(IFNULL(a.discount_code, \'n\')) LIKE \'%influ%\' then 1 when a.order_name IN (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') then 1 when a.tags IN (\'FLITS_LOGICERP\') then 1 when lower(a.tags) like \'%creator%\' then 1 else 0 end as filters, case when a.loyalty_customer = \'YES\' then \'loyal\' when a.order_timestamp::date = b.first_order_date then \'new\' else \'repeat\' end as customer_type from snitch_db.maplemonk.fact_items_snitch a left join snitch_db.maplemonk.customer_acq_date_channel b on \'+91\' || RIGHT(REGEXP_REPLACE(a.phone, \'[^0-9]\', \'\'), 10) = b.phone_no WHERE order_status = \'Shopify_Processed\' and order_timestamp::date >= \'2026-11-24\' ), sales as ( select order_timestamp::date as date, customer_type, sum(gross_sales)::int as overall_sales, sum(case when sku_group in (select sku_group from snitch_db.maplemonk.base_product where product_tags like \'%BFS_2026_MASTER%\') then gross_sales end)::int as discounted_sales, sum(quantity)::int as overall_quantity, sum(case when sku_group in (select sku_group from snitch_db.maplemonk.base_product where product_tags like \'%BFS_2026_MASTER%\') then quantity end)::int as discounted_quant, count(distinct combined_order_name) as overall_orders, count(distinct \'+91\' || RIGHT(REGEXP_REPLACE(phone, \'[^0-9]\', \'\'), 10)) as customers from shopify_pre where order_timestamp::date >= \'2026-11-24\' and filters = 0 group by 1,2 ) select * from sales",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SNITCH_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            