{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.suspicious_orders as select order_date, marketplace_mapped,source,order_name,payment_method,primary_payment_type,secondary_payment_type, sum(selling_price) as gross from snitch_db.snitch.order_lineitems_fact where order_status not in (\'CANCELLED\') and order_date >=date(getdate())-2 and marketplace_mapped in (\'Shopify_India\') group by order_date, marketplace_mapped,source,order_name,payment_method,primary_payment_type,secondary_payment_type order by order_date desc, gross desc",
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
            