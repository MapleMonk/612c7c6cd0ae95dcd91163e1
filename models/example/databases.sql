{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.customer_support as select b.order_date, a.order_name, a.sku, right(replace(a.phone,\' \',\'\'),10) Phone, a.email, b.ordeR_status, b.dispatch_date, c.tracking_url, b.delivered_date, b.return_flag, b.shipping_status, a.is_refund, d.refund_Date from snitch_db.maplemonk.fact_items_snitch a left join snitch_db.maplemonk.unicommerce_fact_items_snitch b on a.order_id=b.order_id and a.line_item_id=split_part(b.saleorderitemcode,\'-\',0) left join ( select id order_id, replace(B.value:sku,\'\"\',\'\') sku, replace(A.value:tracking_url,\'\"\',\'\') tracking_url from Snitch_db.maplemonk.Shopify_All_orders, lateral flatten (input => fulfillments) A, lateral flatten (input=> A.value:line_items) B ) c on a.order_id = c.order_id and a.sku = c.sku left join ( SELECT ID AS Order_ID, C.value:line_item_id AS line_item_id, left(replace(A.value:created_at,\'\"\',\'\'),10)::date refund_Date FROM Snitch_db.maplemonk.Shopify_All_orders, LATERAL FLATTEN(INPUT => refunds)A, LATERAL FLATTEN(INPUT=>A.value)B, LATERAL FLATTEN(INPUT => B.value) C WHERE C.value:line_item_id IS NOT null ) d on a.ordeR_id = d.ordeR_id and a.line_item_id = d.line_item_id ;",
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
                        