{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.ITALY_DSR AS WITH CTE AS (SELECT FI.ORDER_TIMESTAMP::date AS DATE, UPPER(DAYNAME(FI.ORDER_TIMESTAMP::DATE)) AS Day, SUM(CASE WHEN FI.shop_name=\'Shopify_Italy\' THEN FI.Net_Sales_INR END) AS Shopify_Italy_Sales_INR, SUM(CASE WHEN FI.shop_name=\'Shopify_Italy\' THEN FI.total_sales END) AS Shopify_Italy_Sales_EUR FROM Vahdam_db.maplemonk.FACT_ITEMS FI WHERE FI.is_refund=0 GROUP BY FI.ORDER_TIMESTAMP::date ORDER BY FI.ORDER_TIMESTAMP::date DESC) SELECT CTE.*, AZ.AMAZON_ADS_IT_SPEND_SPONSORED_PRODUCTS, AZ.AMAZON_ADS_IT_SPEND_SPONSORED_DISPLAY, AZ.AMAZON_ADS_IT_SPEND_SPONSORED_BRANDS_VIDEO, AZ.AMAZON_ADS_IT_SPEND_SPONSORED_BRANDS, AVP.AMAZON_1P_SALES, AVP.AMAZON_1P_SPEND, AVP.AMAZON_1P_SESSIONS, AVP.AMAZON_1P_UNITS, FG.FACEBOOK_ADS_ITALY_SPEND, AZS.NET_SALES_EUR as AMAZON_ITALY_SALES_EUR, AZS.NET_SALES_INR as Amazon_Italy_Sales_INR, AZS.ORDERS as Amazon_Italy_Orders, AZS.QUANTITY as Amazon_Italy_Quantity FROM CTE LEFT JOIN (SELECT DATE, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Products\' THEN SPEND END) AS AMAZON_ADS_IT_SPEND_SPONSORED_PRODUCTS, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Display\' THEN SPEND END) AS AMAZON_ADS_IT_SPEND_SPONSORED_DISPLAY, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands Video\' THEN SPEND END) AS AMAZON_ADS_IT_SPEND_SPONSORED_BRANDS_VIDEO, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands\' THEN SPEND END) AS AMAZON_ADS_IT_SPEND_SPONSORED_BRANDS FROM Vahdam_db.maplemonk.AMAZONADS_IT_MARKETING GROUP BY DATE) AZ ON CTE.DATE=AZ.DATE LEFT JOIN (SELECT DATE, SUM(SHIPPEDCOGS_Amount) AS AMAZON_1P_SALES, SUM(SPEND) AS AMAZON_1P_SPEND, SUM(SESSIONS) AS AMAZON_1P_SESSIONS, SUM(Shipped_units) AS AMAZON_1P_UNITS, sum(Primary_SHIPPEDCOGS_Amount) as Amazon_1P_Primary_Sales, sum(Primary_Shipped_units) as Amazon_1P_Primary_Shipped_units FROM vahdam_db.maplemonk.amazon1pads_it_marketing GROUP BY DATE) AVP ON CTE.DATE=AVP.DATE LEFT JOIN (SELECT DATE, SUM(CASE WHEN CHANNEL=\'Facebook Ads\' THEN SPEND END) AS FACEBOOK_ADS_ITALY_SPEND FROM Vahdam_db.maplemonk.facebook_italy_consolidated GROUP BY DATE) FG ON CTE.DATE=FG.DATE LEFT JOIN (select AIT.\"purchase-date\"::date AS DATE ,sum(ifnull(try_cast(AIT.\"item-price\" as float),0) - ifnull(try_cast(AIT.\"item-promotion-discount\" as float),0)) as net_sales_eur ,sum((ifnull(try_cast(AIT.\"item-price\" as float),0) - ifnull(try_cast(AIT.\"item-promotion-discount\" as float),0))*EX.RATES:INR::FLOAT) as net_sales_inr ,sum(ifnull(try_cast(AIT.QUANTITY as float), 0)) as Quantity ,count(distinct AIT.\"amazon-order-id\") Orders from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_UK_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL\" AIT LEFT JOIN \"VAHDAM_DB\".\"MAPLEMONK\".\"EXCHANGE_RATES\" EX ON AIT.\"purchase-date\"::date = EX.date where AIT.\"sales-channel\" = \'Amazon.it\' and lower(AIT.\"order-status\") <> \'cancelled\' group by AIT.\"purchase-date\"::date order by AIT.\"purchase-date\"::date desc)AZS ON CTE.DATE=AZS.DATE; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.Germany_DSR AS WITH CTE AS (SELECT FI.ORDER_TIMESTAMP::date AS DATE, UPPER(DAYNAME(FI.ORDER_TIMESTAMP::DATE)) AS Day, SUM(CASE WHEN FI.shop_name=\'Shopify_Germany\' THEN FI.Net_Sales_INR END) AS Shopify_Germany_Sales_INR, SUM(CASE WHEN FI.shop_name=\'Shopify_Germany\' THEN FI.total_sales END) AS Shopify_Germany_Sales_EUR FROM Vahdam_db.maplemonk.FACT_ITEMS FI WHERE FI.is_refund=0 GROUP BY FI.ORDER_TIMESTAMP::date ORDER BY FI.ORDER_TIMESTAMP::date DESC) SELECT CTE.*, AZ.AMAZON_ADS_DE_SPEND_SPONSORED_PRODUCTS, AZ.AMAZON_ADS_DE_SPEND_SPONSORED_DISPLAY, AZ.AMAZON_ADS_DE_SPEND_SPONSORED_BRANDS_VIDEO, AZ.AMAZON_ADS_DE_SPEND_SPONSORED_BRANDS, AVP.AMAZON_1P_SALES, AVP.AMAZON_1P_SPEND, AVP.AMAZON_1P_SESSIONS, AVP.AMAZON_1P_UNITS, FG.GOOGLE_ADS_GERMANY_SPEND, FG.FACEBOOK_ADS_GERMANY_SPEND, AZS.net_sales_eur AS Amazon_Germany_Sales_EUR, AZS.net_sales_inr AS Amazon_Germany_Sales_INR, AZS.ORDERS as Amazon_Germany_Orders, AZS.QUANTITY as Amazon_Germany_Quantity, ASPS.sessions as Sessions_DE FROM CTE LEFT JOIN (SELECT DATE, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Products\' THEN SPEND END) AS AMAZON_ADS_DE_SPEND_SPONSORED_PRODUCTS, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Display\' THEN SPEND END) AS AMAZON_ADS_DE_SPEND_SPONSORED_DISPLAY, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands Video\' THEN SPEND END) AS AMAZON_ADS_DE_SPEND_SPONSORED_BRANDS_VIDEO, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands\' THEN SPEND END) AS AMAZON_ADS_DE_SPEND_SPONSORED_BRANDS FROM Vahdam_db.maplemonk.AMAZONADS_DE_MARKETING GROUP BY DATE) AZ ON CTE.DATE=AZ.DATE LEFT JOIN (SELECT DATE, SUM(SHIPPEDCOGS_Amount) AS AMAZON_1P_SALES, SUM(SPEND) AS AMAZON_1P_SPEND, SUM(SESSIONS) AS AMAZON_1P_SESSIONS, SUM(Shipped_units) AS AMAZON_1P_UNITS, sum(Primary_SHIPPEDCOGS_Amount) as Amazon_1P_Primary_Sales, sum(Primary_Shipped_units) as Amazon_1P_Primary_Shipped_units FROM vahdam_db.maplemonk.amazon1pads_de_marketing GROUP BY DATE) AVP ON CTE.DATE=AVP.DATE LEFT JOIN (SELECT DATE, SUM(CASE WHEN CHANNEL=\'Google Ads\' THEN SPEND END) AS GOOGLE_ADS_GERMANY_SPEND, SUM(CASE WHEN CHANNEL=\'Facebook Ads\' THEN SPEND END) AS FACEBOOK_ADS_GERMANY_SPEND FROM Vahdam_db.maplemonk.GERMANY_MARKETING_CONSOLIDATED GROUP BY DATE) FG ON CTE.DATE=FG.DATE LEFT JOIN ( select ADE.\"purchase-date\"::date AS DATE ,sum(ifnull(try_cast(ADE.\"item-price\" as float),0) - ifnull(try_cast(ADE.\"item-promotion-discount\" as float),0)) as net_sales_eur ,sum((ifnull(try_cast(ADE.\"item-price\" as float),0) - ifnull(try_cast(ADE.\"item-promotion-discount\" as float),0))*EX.RATES:INR::FLOAT) as net_sales_inr ,sum(ifnull(try_cast(ADE.QUANTITY as float), 0)) as Quantity ,count(distinct ADE.\"amazon-order-id\") Orders from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_UK_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL\" ADE LEFT JOIN \"VAHDAM_DB\".\"MAPLEMONK\".\"EXCHANGE_RATES\" EX ON ADE.\"purchase-date\"::date = EX.date where ADE.\"sales-channel\" = \'Amazon.de\' and lower(ADE.\"order-status\") <> \'cancelled\' group by ADE.\"purchase-date\"::date order by ADE.\"purchase-date\"::date desc) AZS ON CTE.DATE=AZS.DATE LEFT JOIN (select datastarttime, sum(trafficbyasin:sessions::float) as sessions, sum(trafficbyasin:pageViews::float) as pageviews from vahdam_db.maplemonk.asp_de_get_sales_and_traffic_report_asin group by 1) ASPS ON CTE.DATE = ASPS.datastarttime ORDER BY CTE.DATE DESC; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.Amazon_France_DSR AS WITH CTE AS (SELECT DATE, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Products\' THEN SPEND END) AS AMAZON_ADS_FR_SPEND_SPONSORED_PRODUCTS, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Display\' THEN SPEND END) AS AMAZON_ADS_FR_SPEND_SPONSORED_DISPLAY, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands Video\' THEN SPEND END) AS AMAZON_ADS_FR_SPEND_SPONSORED_BRANDS_VIDEO, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands\' THEN SPEND END) AS AMAZON_ADS_FR_SPEND_SPONSORED_BRANDS FROM Vahdam_db.maplemonk.AMAZONADS_FR_MARKETING GROUP BY DATE) SELECT CTE.*, AZS.net_sales_eur AS Amazon_France_Sales_EUR, AZS.net_sales_inr AS Amazon_France_Sales_INR, AZS.ORDERS as Amazon_France_Orders, AZS.QUANTITY as Amazon_France_Quantity, ASPS.Sessions as Sessions_FR FROM CTE LEFT JOIN ( select AFR.\"purchase-date\"::date AS DATE ,sum(ifnull(try_cast(AFR.\"item-price\" as float),0) - ifnull(try_cast(AFR.\"item-promotion-discount\" as float),0)) as net_sales_eur ,sum((ifnull(try_cast(AFR.\"item-price\" as float),0) - ifnull(try_cast(AFR.\"item-promotion-discount\" as float),0))*EX.RATES:INR::FLOAT) as net_sales_inr ,sum(ifnull(try_cast(AFR.QUANTITY as float), 0)) as Quantity ,count(distinct AFR.\"amazon-order-id\") Orders from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_UK_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL\" AFR LEFT JOIN \"VAHDAM_DB\".\"MAPLEMONK\".\"EXCHANGE_RATES\" EX ON AFR.\"purchase-date\"::date = EX.date where AFR.\"sales-channel\" = \'Amazon.fr\' and lower(AFR.\"order-status\") <> \'cancelled\' group by AFR.\"purchase-date\"::date order by AFR.\"purchase-date\"::date desc) AZS ON CTE.DATE=AZS.DATE LEFT JOIN (SELECT DATE, SUM(SHIPPEDCOGS_Amount) AS AMAZON_1P_SALES, SUM(SPEND) AS AMAZON_1P_SPEND, SUM(SESSIONS) AS AMAZON_1P_SESSIONS, SUM(Shipped_units) AS AMAZON_1P_UNITS, sum(Primary_SHIPPEDCOGS_Amount) as Amazon_1P_Primary_Sales, sum(Primary_Shipped_units) as Amazon_1P_Primary_Shipped_units FROM vahdam_db.maplemonk.amazon1pads_fr_marketing GROUP BY DATE) AVP ON CTE.DATE=AVP.DATE LEFT JOIN (select DATE, sum(TRAFFICBYDATE:browserSessions::float) browserSessions, sum(TRAFFICBYDATE:mobileAppSessions::float) mobileAppSessions, sum(TRAFFICBYDATE:sessions::float) sessions, sum(TRAFFICBYDATE:browserPageViews::float) browserPageViews, sum(TRAFFICBYDATE:mobileAppPageViews::float) mobileAppPageViews, sum(TRAFFICBYDATE:pageViews::float) pageViews, sum(TRAFFICBYDATE:buyBoxPercentage::float) buyBoxPercentage from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_FR_GET_SALES_AND_TRAFFIC_REPORT_DATE\" group by DATE) ASPS ON CTE.DATE = ASPS.DATE ORDER BY CTE.DATE DESC; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.Amazon_UK_DSR AS WITH CTE AS (SELECT DATE, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Products\' THEN SPEND END) AS AMAZON_ADS_UK_SPEND_SPONSORED_PRODUCTS, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Display\' THEN SPEND END) AS AMAZON_ADS_UK_SPEND_SPONSORED_DISPLAY, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands Video\' THEN SPEND END) AS AMAZON_ADS_UK_SPEND_SPONSORED_BRANDS_VIDEO, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands\' THEN SPEND END) AS AMAZON_ADS_UK_SPEND_SPONSORED_BRANDS FROM Vahdam_db.maplemonk.AMAZONADS_UK_MARKETING GROUP BY DATE) SELECT CTE.*, AZS.net_sales_eur AS Amazon_UK_Sales_EUR, AZS.net_sales_inr AS Amazon_UK_Sales_INR, AZS.ORDERS as Amazon_UK_Orders, AZS.QUANTITY as Amazon_UK_Quantity, ASPS.Sessions as Sessions_UK, AVP.AMAZON_1P_SALES, AVP.AMAZON_1P_SPEND, AVP.AMAZON_1P_SESSIONS, AVP.AMAZON_1P_UNITS FROM CTE LEFT JOIN ( select AUK.\"purchase-date\"::date AS DATE ,sum(ifnull(try_cast(AUK.\"item-price\" as float),0) - ifnull(try_cast(AUK.\"item-promotion-discount\" as float),0)) as net_sales_eur ,sum((ifnull(try_cast(AUK.\"item-price\" as float),0) - ifnull(try_cast(AUK.\"item-promotion-discount\" as float),0))*EX.RATES:INR::FLOAT) as net_sales_inr ,sum(ifnull(try_cast(AUK.QUANTITY as float), 0)) as Quantity ,count(distinct AUK.\"amazon-order-id\") Orders from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_UK_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL\" AUK LEFT JOIN \"VAHDAM_DB\".\"MAPLEMONK\".\"EXCHANGE_RATES\" EX ON AUK.\"purchase-date\"::date = EX.date where AUK.\"sales-channel\" = \'Amazon.co.uk\' and lower(AUK.\"order-status\") <> \'cancelled\' group by AUK.\"purchase-date\"::date order by AUK.\"purchase-date\"::date desc) AZS ON CTE.DATE=AZS.DATE LEFT JOIN (SELECT DATE, SUM(SHIPPEDCOGS_Amount) AS AMAZON_1P_SALES, SUM(SPEND) AS AMAZON_1P_SPEND, SUM(SESSIONS) AS AMAZON_1P_SESSIONS, SUM(Shipped_units) AS AMAZON_1P_UNITS, sum(Primary_SHIPPEDCOGS_Amount) as Amazon_1P_Primary_Sales, sum(Primary_Shipped_units) as Amazon_1P_Primary_Shipped_units FROM vahdam_db.maplemonk.amazon1pads_uk_marketing GROUP BY DATE) AVP ON CTE.DATE=AVP.DATE LEFT JOIN (select DATE, sum(TRAFFICBYDATE:browserSessions::float) browserSessions, sum(TRAFFICBYDATE:mobileAppSessions::float) mobileAppSessions, sum(TRAFFICBYDATE:sessions::float) sessions, sum(TRAFFICBYDATE:browserPageViews::float) browserPageViews, sum(TRAFFICBYDATE:mobileAppPageViews::float) mobileAppPageViews, sum(TRAFFICBYDATE:pageViews::float) pageViews, sum(TRAFFICBYDATE:buyBoxPercentage::float) buyBoxPercentage from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_UK_GET_SALES_AND_TRAFFIC_REPORT_DATE\" group by DATE) ASPS ON CTE.DATE = ASPS.DATE ORDER BY CTE.DATE DESC; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.Amazon_Spain_DSR AS WITH CTE AS (SELECT DATE, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Products\' THEN SPEND END) AS AMAZON_ADS_ESP_SPEND_SPONSORED_PRODUCTS, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Display\' THEN SPEND END) AS AMAZON_ADS_ESP_SPEND_SPONSORED_DISPLAY, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands Video\' THEN SPEND END) AS AMAZON_ADS_ESP_SPEND_SPONSORED_BRANDS_VIDEO, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands\' THEN SPEND END) AS AMAZON_ADS_ESP_SPEND_SPONSORED_BRANDS FROM Vahdam_db.maplemonk.AMAZONADS_ESP_MARKETING GROUP BY DATE) SELECT CTE.*, AZS.net_sales_eur AS Amazon_ESP_Sales_EUR, AZS.net_sales_inr AS Amazon_ESP_Sales_INR, AZS.ORDERS as Amazon_ESP_Orders, AZS.QUANTITY as Amazon_ESP_Quantity, ASPS.Sessions as Sessions_ESP, AVP.AMAZON_1P_SALES, AVP.AMAZON_1P_SPEND, AVP.AMAZON_1P_SESSIONS, AVP.AMAZON_1P_UNITS FROM CTE LEFT JOIN ( select AESP.\"purchase-date\"::date AS DATE ,sum(ifnull(try_cast(AESP.\"item-price\" as float),0) - ifnull(try_cast(AESP.\"item-promotion-discount\" as float),0)) as net_sales_eur ,sum((ifnull(try_cast(AESP.\"item-price\" as float),0) - ifnull(try_cast(AESP.\"item-promotion-discount\" as float),0))*EX.RATES:INR::FLOAT) as net_sales_inr ,sum(ifnull(try_cast(AESP.QUANTITY as float), 0)) as Quantity ,count(distinct AESP.\"amazon-order-id\") Orders from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_UK_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL\" AESP LEFT JOIN \"VAHDAM_DB\".\"MAPLEMONK\".\"EXCHANGE_RATES\" EX ON AESP.\"purchase-date\"::date = EX.date where AESP.\"sales-channel\" = \'Amazon.es\' and lower(AESP.\"order-status\") <> \'cancelled\' group by AESP.\"purchase-date\"::date order by AESP.\"purchase-date\"::date desc) AZS ON CTE.DATE=AZS.DATE LEFT JOIN (SELECT DATE, SUM(SHIPPEDCOGS_Amount) AS AMAZON_1P_SALES, SUM(SPEND) AS AMAZON_1P_SPEND, SUM(SESSIONS) AS AMAZON_1P_SESSIONS, SUM(Shipped_units) AS AMAZON_1P_UNITS, sum(Primary_SHIPPEDCOGS_Amount) as Amazon_1P_Primary_Sales, sum(Primary_Shipped_units) as Amazon_1P_Primary_Shipped_units FROM vahdam_db.maplemonk.amazon1pads_esp_marketing GROUP BY DATE) AVP ON CTE.DATE=AVP.DATE LEFT JOIN (select DATE, sum(TRAFFICBYDATE:browserSessions::float) browserSessions, sum(TRAFFICBYDATE:mobileAppSessions::float) mobileAppSessions, sum(TRAFFICBYDATE:sessions::float) sessions, sum(TRAFFICBYDATE:browserPageViews::float) browserPageViews, sum(TRAFFICBYDATE:mobileAppPageViews::float) mobileAppPageViews, sum(TRAFFICBYDATE:pageViews::float) pageViews, sum(TRAFFICBYDATE:buyBoxPercentage::float) buyBoxPercentage from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_ESP_GET_SALES_AND_TRAFFIC_REPORT_DATE\" group by DATE) ASPS ON CTE.DATE = ASPS.DATE ORDER BY CTE.DATE DESC; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.AMAZON_POL_DSR AS WITH CTE AS (SELECT FI.ORDER_TIMESTAMP::date AS DATE, UPPER(DAYNAME(FI.ORDER_TIMESTAMP::DATE)) AS Day FROM Vahdam_db.maplemonk.FACT_ITEMS FI WHERE FI.is_refund=0 GROUP BY FI.ORDER_TIMESTAMP::date ORDER BY FI.ORDER_TIMESTAMP::date DESC) SELECT CTE.*, AZS.net_sales_PLN AS Amazon_POL_Sales_PLN, AZS.ORDERS as Amazon_POL_Orders, AZS.QUANTITY as Amazon_POL_Quantity FROM CTE LEFT JOIN ( select ADE.\"purchase-date\"::date AS DATE ,sum(ifnull(try_cast(ADE.\"item-price\" as float),0) - ifnull(try_cast(ADE.\"item-promotion-discount\" as float),0)) as net_sales_PLN ,sum(ifnull(try_cast(ADE.QUANTITY as float), 0)) as Quantity ,count(distinct ADE.\"amazon-order-id\") Orders from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_UK_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL\" ADE where ADE.\"sales-channel\" = \'Amazon.pl\' and lower(ADE.\"order-status\") <> \'cancelled\' group by ADE.\"purchase-date\"::date order by ADE.\"purchase-date\"::date desc) AZS ON CTE.DATE=AZS.DATE ORDER BY CTE.DATE DESC; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.AMAZON_NETHERLANDS_DSR AS WITH CTE AS (SELECT FI.ORDER_TIMESTAMP::date AS DATE, UPPER(DAYNAME(FI.ORDER_TIMESTAMP::DATE)) AS Day FROM Vahdam_db.maplemonk.FACT_ITEMS FI WHERE FI.is_refund=0 GROUP BY FI.ORDER_TIMESTAMP::date ORDER BY FI.ORDER_TIMESTAMP::date DESC) SELECT CTE.*, ANL.AMAZON_ADS_NL_SPEND_SPONSORED_PRODUCTS, ANL.AMAZON_ADS_NL_SPEND_SPONSORED_DISPLAY, ANL.AMAZON_ADS_NL_SPEND_SPONSORED_BRANDS_VIDEO, ANL.AMAZON_ADS_NL_SPEND_SPONSORED_BRANDS, AZS.net_sales_ANG AS Amazon_NL_Sales_ANG, AZS.ORDERS as Amazon_NL_Orders, AZS.QUANTITY as Amazon_NL_Quantity FROM CTE LEFT JOIN (SELECT DATE, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Products\' THEN SPEND END) AS AMAZON_ADS_NL_SPEND_SPONSORED_PRODUCTS, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Display\' THEN SPEND END) AS AMAZON_ADS_NL_SPEND_SPONSORED_DISPLAY, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands Video\' THEN SPEND END) AS AMAZON_ADS_NL_SPEND_SPONSORED_BRANDS_VIDEO, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands\' THEN SPEND END) AS AMAZON_ADS_NL_SPEND_SPONSORED_BRANDS FROM Vahdam_db.maplemonk.AMAZONADS_NL_MARKETING GROUP BY DATE) ANL ON CTE.DATE = ANL.DATE LEFT JOIN ( select ADE.\"purchase-date\"::date AS DATE ,sum(ifnull(try_cast(ADE.\"item-price\" as float),0) - ifnull(try_cast(ADE.\"item-promotion-discount\" as float),0)) as net_sales_ANG ,sum(ifnull(try_cast(ADE.QUANTITY as float), 0)) as Quantity ,count(distinct ADE.\"amazon-order-id\") Orders from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_UK_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL\" ADE where ADE.\"sales-channel\" = \'Amazon.nl\' and lower(ADE.\"order-status\") <> \'cancelled\' group by ADE.\"purchase-date\"::date order by ADE.\"purchase-date\"::date desc) AZS ON CTE.DATE=AZS.DATE ORDER BY CTE.DATE DESC; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.AMAZON_SWEDEN_DSR AS WITH CTE AS (SELECT FI.ORDER_TIMESTAMP::date AS DATE, UPPER(DAYNAME(FI.ORDER_TIMESTAMP::DATE)) AS Day FROM Vahdam_db.maplemonk.FACT_ITEMS FI WHERE FI.is_refund=0 GROUP BY FI.ORDER_TIMESTAMP::date ORDER BY FI.ORDER_TIMESTAMP::date DESC) SELECT CTE.*, ASE.AMAZON_ADS_SE_SPEND_SPONSORED_PRODUCTS, ASE.AMAZON_ADS_SE_SPEND_SPONSORED_DISPLAY, ASE.AMAZON_ADS_SE_SPEND_SPONSORED_BRANDS_VIDEO, ASE.AMAZON_ADS_SE_SPEND_SPONSORED_BRANDS, AZS.net_sales_SEK AS Amazon_SE_Sales_SEK, AZS.ORDERS as Amazon_SE_Orders, AZS.QUANTITY as Amazon_SE_Quantity FROM CTE LEFT JOIN (SELECT DATE, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Products\' THEN SPEND END) AS AMAZON_ADS_SE_SPEND_SPONSORED_PRODUCTS, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Display\' THEN SPEND END) AS AMAZON_ADS_SE_SPEND_SPONSORED_DISPLAY, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands Video\' THEN SPEND END) AS AMAZON_ADS_SE_SPEND_SPONSORED_BRANDS_VIDEO, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands\' THEN SPEND END) AS AMAZON_ADS_SE_SPEND_SPONSORED_BRANDS FROM Vahdam_db.maplemonk.AMAZONADS_SE_MARKETING GROUP BY DATE) ASE ON CTE.DATE=ASE.DATE LEFT JOIN ( select ADE.\"purchase-date\"::date AS DATE ,sum(ifnull(try_cast(ADE.\"item-price\" as float),0) - ifnull(try_cast(ADE.\"item-promotion-discount\" as float),0)) as net_sales_SEK ,sum(ifnull(try_cast(ADE.QUANTITY as float), 0)) as Quantity ,count(distinct ADE.\"amazon-order-id\") Orders from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_UK_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL\" ADE where ADE.\"sales-channel\" = \'Amazon.se\' and lower(ADE.\"order-status\") <> \'cancelled\' group by ADE.\"purchase-date\"::date order by ADE.\"purchase-date\"::date desc) AZS ON CTE.DATE=AZS.DATE ORDER BY ASE.DATE DESC; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.Global_DSR AS WITH CTE AS (SELECT FI.ORDER_TIMESTAMP::date AS DATE, UPPER(DAYNAME(FI.ORDER_TIMESTAMP::DATE)) AS Day, SUM(CASE WHEN FI.shop_name=\'Shopify_Global\' THEN FI.total_sales END) AS Shopify_Global_Sales_USD, SUM(CASE WHEN FI.shop_name=\'Shopify_Global\' and FI.customer_flag = \'Repeated\' THEN FI.total_sales END) AS Shopify_Global_Repeat_Sales_USD, COUNT(DISTINCT CASE WHEN FI.shop_name=\'Shopify_Global\' and FI.customer_flag = \'Repeated\' THEN FI.customer_id END) AS Shopify_Global_Repeat_Customers, count(DISTINCT case when FI.shop_name=\'Shopify_Global\' then FI.Order_ID end) AS Shopify_Global_Orders, SUM(CASE WHEN FI.shop_name=\'Shopify_Global\' THEN FI.Net_Sales_INR END) AS Shopify_Global_Sales_INR FROM Vahdam_db.maplemonk.FACT_ITEMS FI WHERE FI.is_refund=0 GROUP BY FI.ORDER_TIMESTAMP::date ORDER BY FI.ORDER_TIMESTAMP::date DESC) SELECT CTE.*, ASE.AMAZON_ADS_SE_SPEND_SPONSORED_PRODUCTS, ASE.AMAZON_ADS_SE_SPEND_SPONSORED_DISPLAY, ASE.AMAZON_ADS_SE_SPEND_SPONSORED_BRANDS_VIDEO, ASE.AMAZON_ADS_SE_SPEND_SPONSORED_BRANDS, ASE.Amazon_SE_Sales_SEK, ASE.Amazon_SE_Orders, ASE.Amazon_SE_Quantity, ANL.AMAZON_ADS_NL_SPEND_SPONSORED_PRODUCTS, ANL.AMAZON_ADS_NL_SPEND_SPONSORED_DISPLAY, ANL.AMAZON_ADS_NL_SPEND_SPONSORED_BRANDS_VIDEO, ANL.AMAZON_ADS_NL_SPEND_SPONSORED_BRANDS, ANL.Amazon_NL_Sales_ANG, ANL.Amazon_NL_Orders, ANL.Amazon_NL_Quantity, AFR.AMAZON_ADS_FR_SPEND_SPONSORED_BRANDS, AFR.AMAZON_FRANCE_SALES_EUR, AFR.AMAZON_FRANCE_QUANTITY, AFR.AMAZON_ADS_FR_SPEND_SPONSORED_PRODUCTS, AFR.AMAZON_ADS_FR_SPEND_SPONSORED_BRANDS_VIDEO, AFR.AMAZON_ADS_FR_SPEND_SPONSORED_DISPLAY, AFR.AMAZON_FRANCE_ORDERS, AESP.AMAZON_ADS_ESP_SPEND_SPONSORED_DISPLAY, AESP.AMAZON_ESP_ORDERS, AESP.AMAZON_ADS_ESP_SPEND_SPONSORED_BRANDS, AESP.AMAZON_ADS_ESP_SPEND_SPONSORED_BRANDS_VIDEO, AESP.AMAZON_ADS_ESP_SPEND_SPONSORED_PRODUCTS, AESP.AMAZON_ESP_SALES_EUR, AESP.AMAZON_ESP_QUANTITY, AUK.AMAZON_ADS_UK_SPEND_SPONSORED_PRODUCTS, AUK.AMAZON_ADS_UK_SPEND_SPONSORED_DISPLAY, AUK.AMAZON_UK_ORDERS, AUK.AMAZON_UK_SALES_EUR, AUK.AMAZON_UK_QUANTITY, AUK.AMAZON_ADS_UK_SPEND_SPONSORED_BRANDS, AUK.AMAZON_ADS_UK_SPEND_SPONSORED_BRANDS_VIDEO, APOL.Amazon_POL_Sales_PLN, APOL.Amazon_POL_Orders, APOL.Amazon_POL_Quantity, AUAE.AMAZON_ADS_UAE_SPEND_SPONSORED_PRODUCTS, AUAE.AMAZON_ADS_UAE_SPEND_SPONSORED_DISPLAY, AUAE.AMAZON_ADS_UAE_SPEND_SPONSORED_BRANDS_VIDEO, AUAE.AMAZON_ADS_UAE_SPEND_SPONSORED_BRANDS, AUAE.Amazon_UAE_Sales_AED, AUAE.Amazon_UAE_Orders, AUAE.Amazon_UAE_Quantity, FG.GOOGLE_ADS_GLOBAL_SPEND, FG.FACEBOOK_ADS_GLOBAL_SPEND FROM CTE LEFT JOIN (SELECT DATE, SUM(CASE WHEN CHANNEL=\'Google Ads\' THEN SPEND END) AS GOOGLE_ADS_GLOBAL_SPEND, SUM(CASE WHEN CHANNEL=\'Facebook Ads\' THEN SPEND END) AS FACEBOOK_ADS_GLOBAL_SPEND FROM Vahdam_db.maplemonk.GLOBAL_MARKETING_CONSOLIDATED GROUP BY DATE) FG ON CTE.DATE=FG.DATE LEFT JOIN VAHDAM_DB.MAPLEMONK.AMAZON_FRANCE_DSR AFR ON CTE.DATE = AFR.DATE LEFT JOIN VAHDAM_DB.MAPLEMONK.AMAZON_SPAIN_DSR AESP ON CTE.DATE = AESP.DATE LEFT JOIN VAHDAM_DB.MAPLEMONK.AMAZON_UK_DSR AUK ON CTE.DATE = AUK.DATE LEFT JOIN VAHDAM_DB.MAPLEMONK.Amazon_NETHERLANDS_DSR ANL ON CTE.DATE = ANL.DATE LEFT JOIN VAHDAM_DB.MAPLEMONK.AMAZON_SWEDEN_DSR ASE ON CTE.DATE = ASE.DATE LEFT JOIN VAHDAM_DB.MAPLEMONK.AMAZON_POL_DSR APOL ON CTE.DATE=APOL.DATE LEFT JOIN VAHDAM_DB.MAPLEMONK.AMAZON_UAE_DSR AUAE ON CTE.DATE=AUAE.DATE ORDER BY CTE.DATE DESC ;",
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
                        