{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.order_status as with Shopify_Data as ( SELECT customer_id, line_item_id, order_name, quantity, gross_sales, order_id, order_timestamp, discount_code, tags, sku, sku_group, order_status from snitch_db.maplemonk.fact_items_snitch ), Clickpost as ( SELECT awb_number, sku_list, order_id, pickup_date, intransit_date, outfordelivery_date, delivery_date, rto_date, payment_method, courier_partner FROM snitch_db.snitch.sleepycat_db_clickpost_fact_items where courier_partner != \'Delhivery Reverse\' and courier_partner != \'Proship B2C Reverse\' ), DTO as (SELECT order_id, sku_list, awb_number, orderplaced_date, pickup_date, intransit_date, outfordelivery_date, delivery_date, courier_partner FROM snitch_db.snitch.sleepycat_db_clickpost_fact_items where courier_partner = \'Delhivery Reverse\' or courier_partner = \'Proship B2C Reverse\' ), main_data as ( SELECT customer_id, line_item_id, order_name, sd.sku, sd.sku_group, sd.quantity, sd.order_status, gross_sales, sd.order_id, order_timestamp, cp.awb_number, cp.pickup_date, cp.intransit_date, cp.outfordelivery_date, cp.delivery_date, cp.rto_date as RTO_Initiated, cp.payment_method, dt.pickup_date as return_pickup_date, dt.intransit_date Return_intransit_date, dt.outfordelivery_date as Return_out_for_delivery, dt.delivery_date as Return_Delivery_date FROM Shopify_Data sd left join CLICKPOST cp on sd.order_name = cp.order_id and ARRAY_CONTAINS(TO_VARIANT((sd.sku)), (cp.sku_list)) left join dto dt on cp.order_id = dt.order_id and ARRAY_CONTAINS(TO_VARIANT((sd.sku)), (dt.sku_list)) where sd.order_timestamp >= \'2024-01-01\' and lower(ifnull(sd.discount_code,\'n\')) not like \'%eco%\' and lower(ifnull(discount_code,\'n\')) not like \'%influ%\' and order_name not in (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') and sd.tags not in (\'FLITS_LOGICERP\') ) select a.*,b.Sale_ORDER_ITEM_STATUS,b.dispatched_timestamp, case when a.RTO_Initiated is not null then quantity else 0 end as rto_quant, case when a.return_pickup_date is not null then quantity else 0 end as dto_quant, case when lower(b.sale_order_item_status) = \'cancelled\' AND b.dispatched_timestamp is null then quantity else 0 end as cancel_quant from main_data a left join snitch_db.maplemonk.unicommerce_fact_items_intermediate b on a.order_id = b.order_id and a.sku = b.sku",
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
            