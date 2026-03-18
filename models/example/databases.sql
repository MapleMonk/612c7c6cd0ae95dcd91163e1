{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.BL_shiprocket_fact_items as select ID, SPLIT(channel_order_id, \'_\')[OFFSET(0)] channel_order_id, zone, UPPER(status) AS status, REPLACE(cast(JSON_EXTRACT_SCALAR(A, \'$.awb\') as string), \'\"\', \'\') AS awb, REPLACE(cast(JSON_EXTRACT_SCALAR(A, \'$.courier\') as string), \'\"\', \'\') AS courier, CAST(JSON_VALUE(awb_data, \'$.charges.freight_charges\') AS string) AS shipping_charges, DATETIME(PARSE_TIMESTAMP(\'%d %b %Y, %I:%M %p\', REPLACE(cast(JSON_EXTRACT_SCALAR(A, \'$.pickedup_timestamp\') as string), \'\"\', \'\')),\'Asia/Kolkata\') AS pickedup_timestamp, DATETIME(TIMESTAMP(nullif(REPLACE(cast(JSON_EXTRACT_SCALAR(A, \'$.pickup_scheduled_date\') as string), \'\"\', \'\'), \'0000-00-00 00:00:00\' )), \'Asia/Kolkata\') pickup_scheduled_date, DATETIME(TIMESTAMP(nullif(REPLACE(cast(JSON_EXTRACT_SCALAR(A, \'$.delivered_date\') as string), \'\"\', \'\'), \'0000-00-00 00:00:00\' )), \'Asia/Kolkata\') delivered_date, REPLACE(cast(JSON_EXTRACT_SCALAR(P, \'$.channel_sku\') as string), \'\"\', \'\') AS channel_sku, coalesce(REPLACE(cast(JSON_EXTRACT_SCALAR(A, \'$.rto_awb\') as string), \'\"\', \'\'), REPLACE(cast(JSON_EXTRACT_SCALAR(A, \'$.return_awb\') as string), \'\"\', \'\')) AS Return_awb, DATETIME(TIMESTAMP(nullif(REPLACE(cast(JSON_EXTRACT_SCALAR(A, \'$.rto_initiated_date\') as string), \'\"\', \'\'), \'0000-00-00 00:00:00\' )), \'Asia/Kolkata\') rto_initiated_date, DATETIME(TIMESTAMP(nullif(REPLACE(cast(JSON_EXTRACT_SCALAR(A, \'$.rto_delivered_date\') as string), \'\"\', \'\'), \'0000-00-00 00:00:00\' )), \'Asia/Kolkata\') rto_delivered_date, DATETIME(TIMESTAMP(nullif(REPLACE(cast(JSON_EXTRACT_SCALAR(A, \'$.awb_assign_date\') as string), \'\"\', \'\'), \'0000-00-00 00:00:00\' )), \'Asia/Kolkata\') awb_assign_date FROM `MAPLEMONK.BL_shiprocket_orders` left join UNNEST(SHIPMENTS) AS A left join UNNEST(products) AS P Qualify row_number() over(partition by REPLACE(cast(JSON_EXTRACT_SCALAR(A, \'$.awb\') as string), \'\"\', \'\'), REPLACE(cast(JSON_EXTRACT_SCALAR(P, \'$.channel_sku\') as string), \'\"\', \'\') order by 1) = 1",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            