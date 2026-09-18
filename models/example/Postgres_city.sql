{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.postgre_total_customer AS SELECT u.name, u.phone_number, MIN(o.delivered_at) AS first_order_date, w.created_at AS wallet_recharge_date FROM postgres_users u LEFT JOIN postgres_orders o ON u.id = o.user_id LEFT JOIN postgres_wallet w ON u.id = w.user_id WHERE u.is_deleted = FALSE GROUP BY u.name, u.phone_number, w.created_at; CREATE OR REPLACE TABLE eggozdb.maplemonk.Raw_repeated_Ordered AS SELECT DATE(o.delivered_at) AS date, u.id AS user_id, o.id AS order_id, u.name, u.phone_number, u.email, p.sku_count, p.short_name, SUM(oi.quantity) AS quantity, o.payable_amount AS payable_amount FROM postgres_orders o JOIN postgres_users u ON o.user_id = u.id JOIN postgres_order_item oi ON oi.order_id = o.id JOIN postgres_product_city_price pcp ON pcp.id = oi.product_city_id JOIN postgres_product p ON p.id = pcp.product_id JOIN postgres_retailer r ON r.id = o.retailer_id GROUP BY DATE(o.delivered_at), u.id, o.id, u.name, u.phone_number, u.email, p.sku_count, p.short_name, o.payable_amount;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from EGGOZDB.MAPLEMONK.Postgres_city
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            