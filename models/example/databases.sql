{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.sale_page_analysis as ( with main_data as ( select order_timestamp::date as date, sum(case when product_tags like \'%Strike-03-Oct%\' then gross_sales end) as slashed_gross_sales, sum(case when product_tags like \'%Buy2-40-Oct%\' then gross_sales end) as buy2_gross_sales, sum(case when product_tags like \'%Flat500-Oct%\' then gross_sales end) as flat500_gross_sales, sum(case when product_tags like \'%Strike-03-Oct%\' then quantity end) as slashed_gross_quantity, sum(case when product_tags like \'%Buy2-40-Oct%\' then quantity end) as buy2_gross_quantity, sum(case when product_tags like \'%Flat500-Oct%\' then quantity end) as flat500_gross_quantity, sum(case when product_tags like \'%Strike-03-Oct%\' then discount end) as slashed_gross_discount, sum(case when product_tags like \'%Buy2-40-Oct%\' then discount end) as buy2_gross_discount, sum(case when product_tags like \'%Flat500-Oct%\' then discount end) as flat500_gross_discount, count(distinct case when product_tags like \'%Strike-03-Oct%\' then order_name end) as slashed_gross_orders, count(distinct case when product_tags like \'%Buy2-40-Oct%\' then order_name end) as buy2_gross_orders, count(distinct case when product_tags like \'%Flat500-Oct%\' then order_name end) as flat500_gross_orders, count(distinct case when product_tags like \'%Strike-03-Oct%\' then customer_id end) as slashed_gross_newcustomers, count(distinct case when product_tags like \'%Buy2-40-Oct%\' then customer_id end) as buy2_gross_newcustomers, count(distinct case when product_tags like \'%Flat500-Oct%\' then customer_id end) as flat500_gross_newcustomers from snitch_db.maplemonk.fact_items_snitch where lower(ifnull(discount_code,\'n\')) not like \'%eco%\' and lower(ifnull(discount_code,\'n\')) not like \'%influ%\' and order_name not in (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') and tags not in (\'FLITS_LOGICERP\') and order_timestamp::date >= \'2024-10-04\' group by 1 ) select * from main_data where date < current_date );",
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
            