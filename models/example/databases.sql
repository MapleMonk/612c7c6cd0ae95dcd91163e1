{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.pincode_courier_rank as WITH NormalizedData AS ( SELECT pincode, state, courier, shipping_courier, count(distinct order_name) as num_orders, dispatch_to_delivery_days_50th_percentile, dispatch_to_delivery_days_90th_percentile, count(distinct order_name) * 1.0 / MAX(count(distinct order_name)) OVER () as norm_num_orders, (1 - dispatch_to_delivery_days_50th_percentile * 1.0 / MAX(dispatch_to_delivery_days_50th_percentile) OVER ()) as norm_dispatch_to_delivery_days_50th_percentile, (1 - dispatch_to_delivery_days_90th_percentile * 1.0 / MAX(dispatch_to_delivery_days_90th_percentile) OVER ()) as norm_dispatch_to_delivery_days_90th_percentile FROM snitch_db.maplemonk.unicommerce_fact_items_snitch WHERE order_status NOT IN (\'CANCELLED\') AND marketplace_mapped = \'SHOPIFY\' AND dispatch_to_delivery_days_50th_percentile IS NOT NULL GROUP BY pincode, state, courier, shipping_courier, dispatch_to_delivery_days_50th_percentile, dispatch_to_delivery_days_90th_percentile ) SELECT pincode, state, courier, shipping_courier, num_orders, dispatch_to_delivery_days_50th_percentile, dispatch_to_delivery_days_90th_percentile, 0.20 * norm_num_orders + 0.60 * norm_dispatch_to_delivery_days_50th_percentile + 0.20 * norm_dispatch_to_delivery_days_90th_percentile AS score, RANK() OVER(PARTITION BY pincode ORDER BY score DESC) as rank FROM NormalizedData ORDER BY pincode, rank;",
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
                        