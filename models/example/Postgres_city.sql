{{ config(
            materialized='table',
                post_hook={
                    "sql": "SELECT u.name, u.phone_number, MIN(o.delivered_at) AS first_order_date, w.created_at AS wallet_recharge_date FROM postgres_users u LEFT JOIN postgres_orders o ON u.id = o.user_id LEFT JOIN postgres_wallet w ON u.id = w.user_id WHERE u.is_deleted = FALSE GROUP BY u.name, u.phone_number, w.created_at;",
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
            