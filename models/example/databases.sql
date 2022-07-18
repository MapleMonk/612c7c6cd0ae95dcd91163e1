{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.Amazon_Canada_DSR AS WITH CTE AS (SELECT DATE, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Products\' THEN SPEND END) AS AMAZON_ADS_CA_SPEND_SPONSORED_PRODUCTS, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Display\' THEN SPEND END) AS AMAZON_ADS_CA_SPEND_SPONSORED_DISPLAY, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands Video\' THEN SPEND END) AS AMAZON_ADS_CA_SPEND_SPONSORED_BRANDS_VIDEO, SUM(CASE WHEN CAMPAIGN_TYPE=\'Sponsored Brands\' THEN SPEND END) AS AMAZON_ADS_CA_SPEND_SPONSORED_BRANDS FROM Vahdam_db.maplemonk.AMAZONADS_CA_MARKETING GROUP BY DATE) SELECT CTE.*, AZS.net_sales_eur AS Amazon_CA_Sales_CAD, AZS.net_sales_inr AS Amazon_CA_Sales_INR, AZS.ORDERS as Amazon_CA_Orders, AZS.QUANTITY as Amazon_CA_Quantity FROM CTE LEFT JOIN ( select ACA.\"purchase-date\"::date AS DATE ,sum(ifnull(try_cast(ACA.\"item-price\" as float),0) - ifnull(try_cast(ACA.\"item-promotion-discount\" as float),0)) as net_sales_eur ,sum((ifnull(try_cast(ACA.\"item-price\" as float),0) - ifnull(try_cast(ACA.\"item-promotion-discount\" as float),0))*EX.INR_CAD) as net_sales_inr ,sum(ifnull(try_cast(ACA.QUANTITY as float), 0)) as Quantity ,count(distinct ACA.\"amazon-order-id\") Orders from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_CA_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL\" ACA LEFT JOIN (select date, (RATES:INR * (RATES:EUR/RATES:CAD)) AS INR_CAD from vahdam_db.maplemonk.exchange_rates) EX ON ACA.\"purchase-date\"::date = EX.date where ACA.\"sales-channel\" = \'Amazon.ca\' and lower(ACA.\"order-status\") <> \'cancelled\' group by ACA.\"purchase-date\"::date order by ACA.\"purchase-date\"::date desc) AZS ON CTE.DATE=AZS.DATE ORDER BY CTE.DATE DESC;",
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
                        