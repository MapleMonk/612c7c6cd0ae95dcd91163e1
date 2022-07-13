{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.India_DSR AS WITH CTE AS (SELECT FI.ORDER_TIMESTAMP::date AS DATE, UPPER(DAYNAME(FI.ORDER_TIMESTAMP::DATE)) AS Day, SUM(CASE WHEN FI.shop_name=\'Shopify_India\' THEN FI.Net_Sales_INR END) AS Shopify_India_Sales_INR FROM Vahdam_db.maplemonk.FACT_ITEMS FI WHERE FI.is_refund=0 GROUP BY FI.ORDER_TIMESTAMP::date ORDER BY FI.ORDER_TIMESTAMP::date DESC) SELECT CTE.*, AIN.AMAZON_ADS_IN_SPEND_SPONSORED_PRODUCTS, AIN.AMAZON_ADS_IN_SPEND_SPONSORED_DISPLAY, AIN.AMAZON_ADS_IN_SPEND_SPONSORED_BRANDS_VIDEO, AIN.AMAZON_ADS_IN_SPEND_SPONSORED_BRANDS, FG.GOOGLE_ADS_INDIA_SPEND, FG.FACEBOOK_ADS_INDIA_SPEND, AZS.net_sales_inr AS Amazon_India_Sales_INR, AZS.ORDERS as Amazon_India_Orders, AZS.QUANTITY as Amazon_India_Quantity FROM CTE LEFT JOIN (SELECT DATE, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Products\' THEN SPEND END) AS AMAZON_ADS_IN_SPEND_SPONSORED_PRODUCTS, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Display\' THEN SPEND END) AS AMAZON_ADS_IN_SPEND_SPONSORED_DISPLAY, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands Video\' THEN SPEND END) AS AMAZON_ADS_IN_SPEND_SPONSORED_BRANDS_VIDEO, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands\' THEN SPEND END) AS AMAZON_ADS_IN_SPEND_SPONSORED_BRANDS FROM Vahdam_db.maplemonk.AMAZONADS_IN_MARKETING GROUP BY DATE) AIN ON CTE.DATE=AIN.DATE LEFT JOIN (SELECT DATE, SUM(CASE WHEN CHANNEL=\'Google Ads\' THEN SPEND END) AS GOOGLE_ADS_INDIA_SPEND, SUM(CASE WHEN CHANNEL=\'Facebook Ads\' THEN SPEND END) AS FACEBOOK_ADS_INDIA_SPEND FROM Vahdam_db.maplemonk.MARKETING_CONSOLIDATED_IN GROUP BY DATE) FG ON CTE.DATE=FG.DATE LEFT JOIN ( select ADE.\"purchase-date\"::date AS DATE ,sum(ifnull(try_cast(ADE.\"item-price\" as float),0) - ifnull(try_cast(ADE.\"item-promotion-discount\" as float),0)) as net_sales_inr ,sum(ifnull(try_cast(ADE.QUANTITY as float), 0)) as Quantity ,count(distinct ADE.\"amazon-order-id\") Orders from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_IN_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL\" ADE where ADE.\"sales-channel\" = \'Amazon.in\' and lower(ADE.\"order-status\") <> \'cancelled\' group by ADE.\"purchase-date\"::date order by ADE.\"purchase-date\"::date desc) AZS ON CTE.DATE=AZS.DATE ORDER BY CTE.DATE DESC;",
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
                        