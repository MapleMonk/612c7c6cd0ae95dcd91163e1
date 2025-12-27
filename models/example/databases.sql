{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE Maplemonk.Zouk_ShopifyNector_Report AS WITH Customers as ( select id, owner_id, cast(TIMESTAMP(created_at) as datetime) AS Created_at, REGEXP_REPLACE(JSON_VALUE(value, \'$.nector_user_customer_id\'),r\'^shopify-\',\'\') AS nector_customer_id, JSON_VALUE(value, \'$.nector_user_mobile\') AS Phone, NULLIF(JSON_VALUE(value, \'$.nector_user_email\'),\'\') AS Email, JSON_VALUE(value, \'$.nector_user_name\') AS Customer_Name, JSON_VALUE(value, \'$.nector_user_tier\') AS User_tier, CAST(JSON_VALUE(value, \'$.nector_user_balance\')AS FLOAT64) AS User_Balance, JSON_VALUE(value, \'$.nector_user_available_balance\') AS User_Available_Balance, JSON_VALUE(value, \'$.nector_user_available_credit_balance\') AS User_Available_Credit_Balance, JSON_VALUE(value, \'$.nector_user_tags\') AS User_Tags, JSON_VALUE(value, \'$.nector_user_dob\') AS User_DOB, JSON_VALUE(value, \'$.nector_full_user_dob\') AS User_Full_DOB, JSON_VALUE(value, \'$.nector_user_doa\') AS User_DOA, JSON_VALUE(value, \'$.nector_full_user_dos\') AS User_Full_DOA, JSON_VALUE(value, \'$.nector_user_repeat_purchaser\') AS User_Repeat_Purchaser From MapleMonk.Zouk_Shopify_metafield_customers c ), orders AS ( SELECT LOWER(email) AS email, COUNT(DISTINCT order_id) AS total_orders FROM MapleMonk.zouk_SHOPIFY_FACT_ITEMS WHERE LOWER(order_status) NOT LIKE \'%cancel%\' GROUP BY 1 ) SELECT c.*, COALESCE(o_email.total_orders, 0) AS total_orders FROM customers c LEFT JOIN orders o_email ON c.email IS NOT NULL AND c.email = o_email.email ;",
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
            