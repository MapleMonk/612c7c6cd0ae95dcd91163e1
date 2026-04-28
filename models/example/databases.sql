{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.PHILIPS_AMAZON_VENDOR_PARTNER_SALES AS SELECT \'AMAZON VC\' AS marketplace, \'AMAZON VC\' AS CHANNEL, \'AMAZON VC\' AS SOURCE, CONCAT( a.Asin, CAST(startDate AS DATE), orderedUnits, CAST(PARSE_JSON(orderedRevenue):amount AS FLOAT), CAST(endDate AS DATE) ) AS ORDER_ID, CONCAT( a.Asin, CAST(startDate AS DATE), orderedUnits, CAST(PARSE_JSON(orderedRevenue):amount AS FLOAT), CAST(endDate AS DATE) ) AS reference_code, CONCAT( a.Asin, CAST(startDate AS DATE), orderedUnits, CAST(PARSE_JSON(orderedRevenue):amount AS FLOAT), CAST(endDate AS DATE) ) AS SALEORDERITEMCODE, CONCAT( a.Asin, CAST(startDate AS DATE), orderedUnits, CAST(PARSE_JSON(orderedRevenue):amount AS FLOAT), CAST(endDate AS DATE) ) AS SALES_ORDER_ITEM_ID, CAST(a.asin AS STRING) AS Asin, CAST(startDate AS TIMESTAMP) AS startTime, CAST(endDate AS TIMESTAMP) AS endTime, CAST(orderedUnits AS INTEGER) AS Ordered_Units, CAST(PARSE_JSON(orderedRevenue):amount AS FLOAT) AS Ordered_Revenue, CAST(shippedUnits AS INTEGER) AS ShippedUnits, CAST(customerReturns AS INTEGER) AS CustomerReturns, ( CAST(PARSE_JSON(orderedRevenue):amount AS FLOAT) / NULLIF(CAST(orderedUnits AS INTEGER), 0) ) * CAST(customerReturns AS INTEGER) AS returned_revenue FROM maplemonk.philips_amazon_vendor_get_vendor_sales_report a;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from philips_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            