{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE Maplemonk.Zouk_ShopifyNector_Report AS WITH Customers AS ( SELECT id, owner_id, CAST(TIMESTAMP(created_at) AS DATETIME) AS Created_at, REGEXP_REPLACE(JSON_VALUE(value, \'$.nector_user_customer_id\'), r\'^shopify-\', \'\') AS nector_customer_id, JSON_VALUE(value, \'$.nector_user_mobile\') AS Phone, NULLIF(JSON_VALUE(value, \'$.nector_user_email\'), \'\') AS Email, JSON_VALUE(value, \'$.nector_user_name\') AS Customer_Name, JSON_VALUE(value, \'$.nector_user_tier\') AS User_tier, SAFE_CAST(JSON_VALUE(value, \'$.nector_user_balance\') AS FLOAT64) AS User_Balance, JSON_VALUE(value, \'$.nector_user_available_balance\') AS User_Available_Balance, JSON_VALUE(value, \'$.nector_user_available_credit_balance\') AS User_Available_Credit_Balance, JSON_VALUE(value, \'$.nector_user_tags\') AS User_Tags, JSON_VALUE(value, \'$.nector_user_dob\') AS User_DOB, JSON_VALUE(value, \'$.nector_full_user_dob\') AS User_Full_DOB, JSON_VALUE(value, \'$.nector_user_doa\') AS User_DOA, JSON_VALUE(value, \'$.nector_full_user_dos\') AS User_Full_DOA, JSON_VALUE(value, \'$.nector_user_repeat_purchaser\') AS User_Repeat_Purchaser FROM `MapleMonk.Zouk_Shopify_metafield_customers` ), Order_Stats AS ( SELECT LOWER(EMAIL) AS email, MARKETPLACE, Source, COUNT(DISTINCT ORDER_ID) OVER(PARTITION BY LOWER(EMAIL)) AS total_orders, ROW_NUMBER() OVER(PARTITION BY LOWER(EMAIL) ORDER BY ORDER_TIMESTAMP DESC) as last_visit_rank FROM `maplemonk.zouk_SHOPIFY_FACT_ITEMS` WHERE LOWER(ORDER_STATUS) NOT LIKE \'%cancel%\' ) SELECT c.*, COALESCE(os.total_orders, 0) AS total_orders, CASE WHEN UPPER(os.MARKETPLACE) LIKE \'%POS%\' THEN os.Source ELSE UPPER(MARKETPLACE) END AS Channel, os.Marketplace FROM Customers c LEFT JOIN Order_Stats os ON LOWER(c.Email) = os.email AND os.last_visit_rank = 1;",
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
            