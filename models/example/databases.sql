{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE VAHDAM_DB.MAPLEMONK.ITALY_DSR AS SELECT FI.ORDER_TIMESTAMP::date AS DATE, UPPER(DAYNAME(FI.ORDER_TIMESTAMP::DATE)) AS Day, SUM(CASE WHEN FI.shop_name=\'Shopify_Italy\' THEN FI.NET_SALES_INR END) AS Italy_Website_Sales, SUM(CASE WHEN FI.shop_name=\'Amazon_IT\' THEN FI.NET_SALES_INR END) AS Amazon_Italy_Sales FROM Vahdam_db.maplemonk.FACT_ITEMS FI WHERE FI.is_refund=0 GROUP BY FI.ORDER_TIMESTAMP::date ORDER BY FI.ORDER_TIMESTAMP::date DESC; CREATE OR REPLACE VAHDAM_DB.MAPLEMONK.GERMANY_DSR AS SELECT FI.ORDER_TIMESTAMP::date AS DATE, UPPER(DAYNAME(FI.ORDER_TIMESTAMP::DATE)) AS Day, SUM(CASE WHEN FI.shop_name=\'Shopify_Germany\' THEN FI.NET_SALES_INR END) AS Germany_Website_Sales, SUM(CASE WHEN FI.shop_name=\'Amazon_DE\' THEN FI.NET_SALES_INR END) AS Amazon_Germany_Sales FROM Vahdam_db.maplemonk.FACT_ITEMS FI WHERE FI.is_refund=0 GROUP BY FI.ORDER_TIMESTAMP::date ORDER BY FI.ORDER_TIMESTAMP::date DESC; CREATE OR REPLACE VAHDAM_DB.MAPLEMONK.GLOBAL_DSR AS SELECT FI.ORDER_TIMESTAMP::date AS DATE, UPPER(DAYNAME(FI.ORDER_TIMESTAMP::DATE)) AS Day, SUM (CASE WHEN FI.shop_name=\'Shopify_Global\' THEN FI.NET_SALES_INR END) AS Global_Website_Sales, SUM(CASE WHEN FI.shop_name=\'Amazon_UK\' THEN FI.NET_SALES_INR END) AS Amazon_UK_Sales, SUM(CASE WHEN FI.shop_name=\'Amazon_ESP\' THEN FI.NET_SALES_INR END) AS Amazon_Spain_Sales, SUM(CASE WHEN FI.shop_name=\'Amazon_FR\' THEN FI.NET_SALES_INR END) AS Amazon_France_Sales FROM Vahdam_db.maplemonk.FACT_ITEMS FI WHERE FI.is_refund=0 GROUP BY FI.ORDER_TIMESTAMP::date ORDER BY FI.ORDER_TIMESTAMP::date DESC",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from VAHDAM_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        