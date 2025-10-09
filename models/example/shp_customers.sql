{{ config(
            materialized='table',
                post_hook={
                    "sql": "SELECT o.id AS order_id, o.order_date, c.name AS customer_name, f.status AS fulfillment_status, o.total_amount FROM Orders o LEFT JOIN Customers c ON o.customer_id = c.id LEFT JOIN Fulfillments f ON o.id = f.order_id WHERE f.status = \'delivered\';",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from ghc_db.MAPLEMONK.shp_customers
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            