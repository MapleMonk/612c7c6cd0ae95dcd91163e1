{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE qurez-wh.MAPLEMONK.qurez_AMAZON_VENDOR_PARTNER_SALES AS SELECT \'AMAZON VC\' AS marketplace, \'AMAZON VC QUREZ\' AS CHANNEL, \'AMAZON VC QUREZ\' AS SOURCE, concat(Asin,CAST(startDate AS DATE),orderedUnits,CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64),CAST(endDate AS DATE)) AS ORDER_ID, concat(Asin,CAST(startDate AS DATE),orderedUnits,CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64),CAST(endDate AS DATE)) AS reference_code, concat(Asin,CAST(startDate AS DATE),orderedUnits,CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64),CAST(endDate AS DATE)) AS SALEORDERITEMCODE, concat(Asin,CAST(startDate AS DATE),orderedUnits,CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64),CAST(endDate AS DATE)) AS SALES_ORDER_ITEM_ID, CAST(asin AS STRING) AS Asin, CAST(startDate AS TIMESTAMP) AS startTime, CAST(endDate AS TIMESTAMP) AS endTime, CAST(orderedUnits AS INT64) AS Ordered_Units, CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64) AS Ordered_Revenue, CAST(shippedUnits AS INT64) AS ShippedUnits, CAST(customerReturns AS INT64) AS CustomerReturns, safe_divide(CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64) ,CAST(orderedUnits AS INT64)) * CAST(customerReturns AS INT64) returned_revenue, sk.product_name as product_name_final, sk.category as product_category, sk.sku_code as commonsku FROM `MapleMonk.qurez_amazon_GET_VENDOR_SALES_REPORT` v LEFT JOIN ( SELECT * FROM `qurez-wh.maplemonk.Qurez_SKU_Master` QUALIFY ROW_NUMBER() OVER ( PARTITION BY UPPER(product_id) ORDER BY product_id ) = 1 ) sk ON UPPER(sk.product_id) = UPPER(v.asin) ;",
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
            