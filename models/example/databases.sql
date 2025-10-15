{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MapleMonk.zouk_sales_personalised_tag_brown` AS SELECT Order_Date, reference_code, PRODUCT_NAME, SUM(Quantity) AS Total_Quantity, Payment_Mode FROM `MapleMonk.zouk_sales_consolidated` WHERE LOWER(PRODUCT_NAME) = \'personalised tag (brown)\' GROUP BY Order_Date, reference_code, PRODUCT_NAME, Payment_Mode;",
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
            