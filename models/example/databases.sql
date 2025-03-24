{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.tier_level_acquisation as with pincode as ( select order_timestamp::date as order_date, pincode, case when lower(new_customer_flag) = \'new\' then \'new\' else \'repeat\' end as customer_flag, payment_channel, sum(quantity) as quantity, sum(gross_sales) as gross_sales, sum(discount) as discount, count(distinct order_name) as orders, count(distinct customer_id) as customers from snitch_db.maplemonk.fact_items_snitch WHERE LOWER(IFNULL(discount_code, \'n\')) NOT LIKE \'%eco%\' AND LOWER(IFNULL(discount_code, \'n\')) NOT LIKE \'%influ%\' AND order_name NOT IN (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') AND tags NOT IN (\'FLITS_LOGICERP\') group by 1,2,3,4 ), pincode_mapping as ( select * from snitch_db.maplemonk.gs_pincode_mapping_lat_long_tier qualify row_number() over (partition by pincode order by latitude desc) = 1 ) select a.*, state, tier, district, latitude, longitude from pincode a left join pincode_mapping b on a.pincode::varchar = b.pincode::varchar ;",
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
            