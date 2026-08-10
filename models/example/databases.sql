{{ config(
            materialized='table',
                post_hook={
                    "sql": "drop table if exists public.anveshan_shiprocket_fact_items; create table public.anveshan_shiprocket_fact_items as select * from (select id as shiprocket_id, channel_name, upper(customer_city) as customer_city, upper(customer_state) as customer_state, customer_pincode, upper(customer_name) as customer_name, upper(customer_email) as customer_email, upper(customer_phone) as customer_phone, eway_bill_number, upper(pickup_location) as pickup_location, shipping_method, channel_order_id, cast(product_quantity as int4) as product_quantity, upper(payment_method) as payment_method, payment_status, cast(cod as INT2) as is_cod, sla, cast(tax as DOUBLE PRECISION) as tax, upper(cast(zone as varchar)) as zone, upper(cast(boxes as varchar)) as boxes, cast(total as double precision) as total_sales, is_b2b, upper(trim(status)) as shiprocket_status, master_status as fulfillment_master_status, date(rto_edd) as rto_edd, rto_reason, manifest_id, cast(discount as double precision) as discount, cast(is_return as int2) as is_return, order_tag, shipments[0].id::BIGINT AS shipment_id, shipments[0].courier::varchar AS courier, shipments[0].sr_courier_name::varchar AS courier_name, shipments[0].awb::varchar AS awb, products[0].name::varchar AS product_name, products[0].channel_sku::varchar AS channel_sku, to_timestamp(nullif(etd_date::varchar, \'\'), \'DD-MM-YYYY HH24:MI:SS\') AS etc_date, to_timestamp(nullif(delivered_date::varchar, \'\'), \'DD-MM-YYYY HH24:MI:SS\') AS delivered_date, to_timestamp(nullif(picked_up_date::varchar, \'\'), \'DD-MM-YYYY HH24:MI:SS\') AS picked_up_date, to_timestamp(nullif(out_for_delivery_date::varchar, \'\'), \'DD-MM-YYYY HH24:MI:SS\') AS out_for_delivery_date, to_timestamp(nullif(shipments[0].shipped_date::varchar, \'\'), \'YYYY-MM-DD HH24:MI:SS\') AS shipped_date, to_timestamp(nullif(shipments[0].pickup_scheduled_date::varchar, \'\'), \'YYYY-MM-DD HH24:MI:SS\') AS pickup_scheduled_date, to_timestamp(nullif(shipments[0].awb_assign_date::varchar, \'\'), \'YYYY-MM-DD HH24:MI:SS\') AS awb_assign_date, row_number() over (partition by channel_order_id,shipments[0].awb::varchar order by to_timestamp(nullif(updated_at::varchar, \'\'), \'DD-MM-YYYY HH24:MI:SS\') desc) as rw FROM public.shiprocket_anveshan_shiprocket_orders ) where rw = 1 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            