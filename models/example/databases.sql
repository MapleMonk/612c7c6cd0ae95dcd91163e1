{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.AMAZON_SGP_DSR AS WITH CTE AS (SELECT FI.ORDER_TIMESTAMP::date AS DATE, UPPER(DAYNAME(FI.ORDER_TIMESTAMP::DATE)) AS Day FROM Vahdam_db.maplemonk.FACT_ITEMS FI WHERE FI.is_refund=0 GROUP BY FI.ORDER_TIMESTAMP::date ORDER BY FI.ORDER_TIMESTAMP::date DESC) SELECT CTE.*, ASE.AMAZON_ADS_SGP_SPEND_SPONSORED_PRODUCTS, ASE.AMAZON_ADS_SGP_SPEND_SPONSORED_DISPLAY, ASE.AMAZON_ADS_SGP_SPEND_SPONSORED_BRANDS_VIDEO, ASE.AMAZON_ADS_SGP_SPEND_SPONSORED_BRANDS, AZS.net_sales_SGD AS Amazon_SGP_Sales_SGD, AZS.ORDERS as Amazon_SGP_Orders, AZS.QUANTITY as Amazon_SGP_Quantity, ASPS.Sessions as Sessions_SGP FROM CTE LEFT JOIN (SELECT DATE, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Products\' THEN SPEND END) AS AMAZON_ADS_SGP_SPEND_SPONSORED_PRODUCTS, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Display\' THEN SPEND END) AS AMAZON_ADS_SGP_SPEND_SPONSORED_DISPLAY, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands Video\' THEN SPEND END) AS AMAZON_ADS_SGP_SPEND_SPONSORED_BRANDS_VIDEO, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands\' THEN SPEND END) AS AMAZON_ADS_SGP_SPEND_SPONSORED_BRANDS FROM Vahdam_db.maplemonk.AMAZONADS_SGP_MARKETING GROUP BY DATE) ASE ON CTE.DATE=ASE.DATE LEFT JOIN ( select ADE.\"purchase-date\"::date AS DATE ,sum(ifnull(try_cast(ADE.\"item-price\" as float),0) - ifnull(try_cast(ADE.\"item-promotion-discount\" as float),0)) as net_sales_SGD ,sum(ifnull(try_cast(ADE.QUANTITY as float), 0)) as Quantity ,count(distinct ADE.\"amazon-order-id\") Orders from \"VAHDAM_DB\".\"MAPLEMONK\".\"AA_SG_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL\" ADE where ADE.\"sales-channel\" = \'Amazon.sg\' and lower(ADE.\"order-status\") <> \'cancelled\' group by ADE.\"purchase-date\"::date order by ADE.\"purchase-date\"::date desc) AZS ON CTE.DATE=AZS.DATE LEFT JOIN (select DATE, sum(TRAFFICBYDATE:browserSessions::float) browserSessions, sum(TRAFFICBYDATE:mobileAppSessions::float) mobileAppSessions, sum(TRAFFICBYDATE:sessions::float) sessions, sum(TRAFFICBYDATE:browserPageViews::float) browserPageViews, sum(TRAFFICBYDATE:mobileAppPageViews::float) mobileAppPageViews, sum(TRAFFICBYDATE:pageViews::float) pageViews, sum(TRAFFICBYDATE:buyBoxPercentage::float) buyBoxPercentage from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_SGP_GET_SALES_AND_TRAFFIC_REPORT_DATE\" group by DATE) ASPS ON CTE.DATE = ASPS.DATE ORDER BY CTE.DATE DESC;",
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
                        