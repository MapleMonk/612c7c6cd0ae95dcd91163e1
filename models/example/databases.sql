{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.Amazon_Canada_DSR AS WITH CTE AS (SELECT DATE, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Products\' THEN SPEND END) AS AMAZON_ADS_CA_SPEND_SPONSORED_PRODUCTS, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Display\' THEN SPEND END) AS AMAZON_ADS_CA_SPEND_SPONSORED_DISPLAY, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands Video\' THEN SPEND END) AS AMAZON_ADS_CA_SPEND_SPONSORED_BRANDS_VIDEO, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands\' THEN SPEND END) AS AMAZON_ADS_CA_SPEND_SPONSORED_BRANDS FROM Vahdam_db.maplemonk.AMAZONADS_CA_MARKETING GROUP BY DATE) SELECT CTE.*, AZS.net_sales_eur AS Amazon_CA_Sales_CAD, AZS.net_sales_inr AS Amazon_CA_Sales_INR, AZS.ORDERS as Amazon_CA_Orders, AZS.QUANTITY as Amazon_CA_Quantity, ASPS.sessions as sessions_ca FROM CTE LEFT JOIN ( select convert_timezone(\'UTC\', \'America/Los_Angeles\', ACA.\"purchase-date\"::datetime)::date AS DATE ,sum(ifnull(try_cast(ACA.\"item-price\" as float),0) - ifnull(try_cast(ACA.\"item-promotion-discount\" as float),0)) as net_sales_eur ,sum((ifnull(try_cast(ACA.\"item-price\" as float),0) - ifnull(try_cast(ACA.\"item-promotion-discount\" as float),0))*EX.INR_CAD) as net_sales_inr ,sum(ifnull(try_cast(ACA.QUANTITY as float), 0)) as Quantity ,count(distinct ACA.\"amazon-order-id\") Orders from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_CA_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL\" ACA LEFT JOIN (select date, (RATES:INR * (RATES:EUR/RATES:CAD)) AS INR_CAD from vahdam_db.maplemonk.exchange_rates) EX ON convert_timezone(\'UTC\', \'America/Los_Angeles\', ACA.\"purchase-date\"::datetime)::date = EX.date where ACA.\"sales-channel\" = \'Amazon.ca\' and lower(ACA.\"order-status\") <> \'cancelled\' group by convert_timezone(\'UTC\', \'America/Los_Angeles\', ACA.\"purchase-date\"::datetime)::date order by convert_timezone(\'UTC\', \'America/Los_Angeles\', ACA.\"purchase-date\"::datetime)::date desc) AZS ON CTE.DATE=AZS.DATE LEFT JOIN (select DATE, sum(TRAFFICBYDATE:browserSessions::float) browserSessions, sum(TRAFFICBYDATE:mobileAppSessions::float) mobileAppSessions, sum(TRAFFICBYDATE:sessions::float) sessions, sum(TRAFFICBYDATE:browserPageViews::float) browserPageViews, sum(TRAFFICBYDATE:mobileAppPageViews::float) mobileAppPageViews, sum(TRAFFICBYDATE:pageViews::float) pageViews, sum(TRAFFICBYDATE:buyBoxPercentage::float) buyBoxPercentage from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_CA_GET_SALES_AND_TRAFFIC_REPORT_DATE\" group by DATE) ASPS ON CTE.DATE = ASPS.DATE ORDER BY CTE.DATE DESC; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.Amazon_MEXICO_DSR AS WITH CTE AS (SELECT DATE, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Products\' THEN SPEND END) AS AMAZON_ADS_MX_SPEND_SPONSORED_PRODUCTS, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Display\' THEN SPEND END) AS AMAZON_ADS_MX_SPEND_SPONSORED_DISPLAY, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands Video\' THEN SPEND END) AS AMAZON_ADS_MX_SPEND_SPONSORED_BRANDS_VIDEO, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands\' THEN SPEND END) AS AMAZON_ADS_MX_SPEND_SPONSORED_BRANDS FROM Vahdam_db.maplemonk.AMAZONADS_MX_MARKETING GROUP BY DATE) SELECT CTE.*, AZS.net_sales_mx AS Amazon_Mexico_Sales_MXN, AZS.ORDERS as Amazon_Mexico_Orders, AZS.QUANTITY as Amazon_Mexico_Quantity FROM CTE LEFT JOIN ( select convert_timezone(\'UTC\', \'America/Mexico_city\', AMX.\"purchase-date\"::datetime)::date AS DATE ,sum(ifnull(try_cast(AMX.\"item-price\" as float),0) - ifnull(try_cast(AMX.\"item-promotion-discount\" as float),0)) as net_sales_mx ,sum(ifnull(try_cast(AMX.QUANTITY as float), 0)) as Quantity ,count(distinct AMX.\"amazon-order-id\") Orders from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_USA_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL\" AMX where AMX.\"sales-channel\" = \'Amazon.com.mx\' and lower(AMX.\"order-status\") <> \'cancelled\' group by convert_timezone(\'UTC\', \'America/Mexico_city\', AMX.\"purchase-date\"::datetime)::date order by convert_timezone(\'UTC\', \'America/Mexico_city\', AMX.\"purchase-date\"::datetime)::date desc) AZS ON CTE.DATE=AZS.DATE ORDER BY CTE.DATE DESC; CREATE OR REPLACE TABLE Vahdam_db.maplemonk.DSR AS WITH CTE AS (SELECT OI.ORDER_TIMESTAMP::date AS DATE, UPPER(DAYNAME(OI.ORDER_TIMESTAMP::DATE)) AS Day, SUM(CASE WHEN OI.shop_name=\'Shopify_India\' THEN OI.TOTAL_SALES END) AS Shopify_India_Sales, SUM(CASE WHEN OI.shop_name=\'Shopify_Germany\' THEN OI.TOTAL_SALES END) AS Shopify_Germany_Sales, SUM(CASE WHEN OI.shop_name=\'Shopify_Italy\' THEN OI.TOTAL_SALES END) AS Shopify_Italy_Sales, SUM(CASE WHEN OI.shop_name=\'Shopify_USA_Wholesale\' THEN OI.TOTAL_SALES END) AS Shopify_USA_Wholesale_Sales, SUM(CASE WHEN OI.shop_name=\'Shopify_USA\' THEN OI.TOTAL_SALES END) AS Shopify_USA_Sales, SUM(CASE WHEN OI.shop_name=\'Shopify_Global\' THEN OI.TOTAL_SALES END) AS Shopify_Global_Sales FROM Vahdam_db.maplemonk.Shopify_All_orders_items OI WHERE OI.is_refund=0 GROUP BY OI.ORDER_TIMESTAMP::date ORDER BY OI.ORDER_TIMESTAMP::date DESC) SELECT CTE.*, AZ.AMAZON_ADS_US_SPEND_SPONSORED_PRODUCTS, AZ.AMAZON_ADS_US_SPEND_SPONSORED_DISPLAY, AZ.AMAZON_ADS_US_SPEND_SPONSORED_BRANDS_VIDEO, AZ.AMAZON_ADS_US_SPEND_SPONSORED_BRANDS, FG.GOOGLE_ADS_US_SPEND, FG.FACEBOOK_ADS_US_SPEND, AZS.NET_SALES AS AMAZON_USA_SALES, AZS.ORDERS AS AMAZON_USA_ORDERS, AZS.UNITS AS AMAZON_USA_QUANTITY, AMX.Amazon_Mexico_Sales_MXN, AMX.Amazon_Mexico_Quantity, AMX.Amazon_Mexico_Orders, AMX.AMAZON_ADS_MX_SPEND_SPONSORED_PRODUCTS, AMX.AMAZON_ADS_MX_SPEND_SPONSORED_DISPLAY, AMX.AMAZON_ADS_MX_SPEND_SPONSORED_BRANDS_VIDEO, AMX. AMAZON_ADS_MX_SPEND_SPONSORED_BRANDS, ASPS.sessions as sessions_us, ASPS.pageViews as pageviews_us FROM CTE LEFT JOIN (SELECT DATE, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Products\' THEN SPEND END) AS AMAZON_ADS_US_SPEND_SPONSORED_PRODUCTS, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Display\' THEN SPEND END) AS AMAZON_ADS_US_SPEND_SPONSORED_DISPLAY, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands Video\' THEN SPEND END) AS AMAZON_ADS_US_SPEND_SPONSORED_BRANDS_VIDEO, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands\' THEN SPEND END) AS AMAZON_ADS_US_SPEND_SPONSORED_BRANDS FROM Vahdam_db.maplemonk.AMAZONADS_NA_MARKETING GROUP BY DATE) AZ ON CTE.DATE=AZ.DATE LEFT JOIN (SELECT DATE, SUM(CASE WHEN CHANNEL=\'Google Ads\' THEN SPEND END) AS GOOGLE_ADS_US_SPEND, SUM(CASE WHEN CHANNEL=\'Facebook Ads\' THEN SPEND END) AS FACEBOOK_ADS_US_SPEND FROM Vahdam_db.maplemonk.MARKETING_CONSOLIDATED GROUP BY DATE) FG ON CTE.DATE=FG.DATE LEFT JOIN (SELECT \"Purchase-datetime-PDT\"::DATE AS DATE, COUNT(DISTINCT \"amazon-order-id\") Orders, SUM(TRY_CAST(QUANTITY AS FLOAT)) Units, SUM(TRY_CAST(\"item-promotion-discount\" AS FLOAT)), SUM(TRY_CAST(\"item-tax\" AS FLOAT)), SUM(TRY_CAST(\"item-price\" AS FLOAT)) AS SALES, SUM(TRY_CAST(\"item-price\" AS FLOAT)) - SUM(TRY_CAST(\"item-promotion-discount\" AS FLOAT)) AS NET_SALES FROM (SELECT *, CONVERT_TIMEZONE(\'UTC\',\'America/Los_Angeles\', \"purchase-date\":: DATETIME) as \"Purchase-datetime-PDT\" FROM Vahdam_db.maplemonk.ASP_USA_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL)X WHERE \"Purchase-datetime-PDT\"::DATE >=\'2022-02-01\' AND CURRENCY = \'USD\' AND \"order-status\" NOT IN (\'Cancelled\') AND \"item-price\" NOT IN(\'\',\'0.0\') GROUP BY \"Purchase-datetime-PDT\"::DATE ORDER BY \"Purchase-datetime-PDT\"::DATE)AZS ON CTE.DATE=AZS.DATE LEFT JOIN VAHDAM_DB.MAPLEMONK.Amazon_MEXICO_DSR AMX ON CTE.DATE= AMX.DATE LEFT JOIN (select DATE, sum(TRAFFICBYDATE:browserSessions::float) browserSessions, sum(TRAFFICBYDATE:mobileAppSessions::float) mobileAppSessions, sum(TRAFFICBYDATE:sessions::float) sessions, sum(TRAFFICBYDATE:browserPageViews::float) browserPageViews, sum(TRAFFICBYDATE:mobileAppPageViews::float) mobileAppPageViews, sum(TRAFFICBYDATE:pageViews::float) pageViews, sum(TRAFFICBYDATE:buyBoxPercentage::float) buyBoxPercentage from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_USA_GET_SALES_AND_TRAFFIC_REPORT_DATE\" group by DATE) ASPS ON CTE.DATE = ASPS.DATE ORDER BY CTE.DATE DESC;",
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
                        