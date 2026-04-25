{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.prolicious_AMAZON_VENDOR_PARTNER_SALES AS SELECT \'AMAZON VC\' AS marketplace, \'AMAZON VC\' AS CHANNEL, \'AMAZON VC\' AS SOURCE, concat(Asin,CAST(startDate AS DATE),orderedUnits,CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64),CAST(endDate AS DATE)) AS ORDER_ID, concat(Asin,CAST(startDate AS DATE),orderedUnits,CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64),CAST(endDate AS DATE)) AS reference_code, concat(Asin,CAST(startDate AS DATE),orderedUnits,CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64),CAST(endDate AS DATE)) AS SALEORDERITEMCODE, concat(Asin,CAST(startDate AS DATE),orderedUnits,CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64),CAST(endDate AS DATE)) AS SALES_ORDER_ITEM_ID, CAST(asin AS STRING) AS asin, CAST(startDate AS TIMESTAMP) AS startTime, CAST(endDate AS TIMESTAMP) AS endTime, CAST(orderedUnits AS INT64) AS Ordered_Units, CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64) AS Ordered_Revenue, CAST(shippedUnits AS INT64) AS ShippedUnits, CAST(customerReturns AS INT64) AS CustomerReturns, safe_divide(CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64) ,CAST(orderedUnits AS INT64)) * CAST(customerReturns AS INT64) returned_revenue, p.amazon_sku, p.master_sku, p.product_name, p.category, p.sub_category, p.style, p.nature FROM MapleMonk.Prolicious_AmazonVC_GET_VENDOR_SALES_REPORT fi LEFT JOIN (SELECT amazon_sku, master_sku, product_name, category, sub_category, style, nature from maplemonk.final_SKU_MASTER qualify row_number() over (partition by amazon_sku order by master_sku) = 1 ) p on Trim(upper(CAST(p.amazon_sku as STRING))) = Trim(upper(CAST(fi.asin as STRING))) ;",
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
            