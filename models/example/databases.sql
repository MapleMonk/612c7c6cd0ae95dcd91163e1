{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE APPLDATABASE.MAPLEMONK.alphanso_AMAZON_VENDOR_PARTNER_SALES AS SELECT \'AMAZON VC\' AS marketplace, \'AMAZON VC ALPHANSO\' AS CHANNEL, \'AMAZON VC ALPHANSO\' AS SOURCE, CONCAT(Asin, CAST(startDate AS DATE), orderedUnits, CAST(PARSE_JSON(orderedRevenue):amount AS FLOAT), CAST(endDate AS DATE)) AS ORDER_ID, CONCAT(Asin, CAST(startDate AS DATE), orderedUnits, CAST(PARSE_JSON(orderedRevenue):amount AS FLOAT), CAST(endDate AS DATE)) AS reference_code, CONCAT(Asin, CAST(startDate AS DATE), orderedUnits, CAST(PARSE_JSON(orderedRevenue):amount AS FLOAT), CAST(endDate AS DATE)) AS SALEORDERITEMCODE, CONCAT(Asin, CAST(startDate AS DATE), orderedUnits, CAST(PARSE_JSON(orderedRevenue):amount AS FLOAT), CAST(endDate AS DATE)) AS SALES_ORDER_ITEM_ID, CAST(asin AS STRING) AS Asin, CAST(startDate AS TIMESTAMP) AS startTime, CAST(endDate AS TIMESTAMP) AS endTime, CAST(orderedUnits AS INT) AS Ordered_Units, CAST(PARSE_JSON(orderedRevenue):amount AS FLOAT) AS Ordered_Revenue, CAST(shippedUnits AS INT) AS ShippedUnits, CAST(customerReturns AS INT) AS CustomerReturns, DIV0NULL(CAST(PARSE_JSON(orderedRevenue):amount AS FLOAT), CAST(orderedUnits AS INT)) * CAST(customerReturns AS INT) AS returned_revenue FROM APPLDATABASE.MapleMonk.alphanso_amazon_GET_VENDOR_SALES_REPORT v ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from APPLDATABASE.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            