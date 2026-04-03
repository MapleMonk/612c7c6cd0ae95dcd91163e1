{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table asaya-data-room-487110.Maplemonk.amz_SALES_AND_TRAFFIC_REPORT_ASIN as (SELECT _airbyte_unique_key, sku, childAsin, parentAsin, dataStartTime, CAST(JSON_EXTRACT_SCALAR(salesByAsin, \'$.totalOrderItems\') AS INT64) AS totalOrderItems, CAST(JSON_EXTRACT_SCALAR(salesByAsin, \'$.unitsOrdered\') AS INT64) AS unitsOrdered, CAST(JSON_EXTRACT_SCALAR(salesByAsin, \'$.totalOrderItemsB2B\') AS INT64) AS totalOrderItemsB2B, CAST(JSON_EXTRACT_SCALAR(salesByAsin, \'$.unitsOrderedB2B\') AS INT64) AS unitsOrderedB2B, CAST(JSON_EXTRACT_SCALAR(salesByAsin, \'$.orderedProductSales.amount\') AS FLOAT64) AS orderedProductSales_amount, JSON_EXTRACT_SCALAR(salesByAsin, \'$.orderedProductSales.currencyCode\') AS orderedProductSales_currency, CAST(JSON_EXTRACT_SCALAR(salesByAsin, \'$.orderedProductSalesB2B.amount\') AS FLOAT64) AS orderedProductSalesB2B_amount, JSON_EXTRACT_SCALAR(salesByAsin, \'$.orderedProductSalesB2B.currencyCode\') AS orderedProductSalesB2B_currency, CAST(JSON_EXTRACT_SCALAR(trafficByAsin, \'$.sessions\') AS INT64) AS sessions, CAST(JSON_EXTRACT_SCALAR(trafficByAsin, \'$.sessionsB2B\') AS INT64) AS sessionsB2B, CAST(JSON_EXTRACT_SCALAR(trafficByAsin, \'$.pageViews\') AS INT64) AS pageViews, CAST(JSON_EXTRACT_SCALAR(trafficByAsin, \'$.pageViewsB2B\') AS INT64) AS pageViewsB2B, CAST(JSON_EXTRACT_SCALAR(trafficByAsin, \'$.browserSessions\') AS INT64) AS browserSessions, CAST(JSON_EXTRACT_SCALAR(trafficByAsin, \'$.mobileAppSessions\') AS INT64) AS mobileAppSessions, CAST(JSON_EXTRACT_SCALAR(trafficByAsin, \'$.browserPageViews\') AS INT64) AS browserPageViews, CAST(JSON_EXTRACT_SCALAR(trafficByAsin, \'$.mobileAppPageViews\') AS INT64) AS mobileAppPageViews, CAST(JSON_EXTRACT_SCALAR(trafficByAsin, \'$.buyBoxPercentage\') AS FLOAT64) AS buyBoxPercentage, CAST(JSON_EXTRACT_SCALAR(trafficByAsin, \'$.buyBoxPercentageB2B\') AS FLOAT64) AS buyBoxPercentageB2B, CAST(JSON_EXTRACT_SCALAR(trafficByAsin, \'$.unitSessionPercentage\') AS FLOAT64) AS unitSessionPercentage, CAST(JSON_EXTRACT_SCALAR(trafficByAsin, \'$.unitSessionPercentageB2B\') AS FLOAT64) AS unitSessionPercentageB2B, CAST(JSON_EXTRACT_SCALAR(trafficByAsin, \'$.sessionPercentage\') AS FLOAT64) AS sessionPercentage, CAST(JSON_EXTRACT_SCALAR(trafficByAsin, \'$.sessionPercentageB2B\') AS FLOAT64) AS sessionPercentageB2B, CAST(JSON_EXTRACT_SCALAR(trafficByAsin, \'$.pageViewsPercentage\') AS FLOAT64) AS pageViewsPercentage, CAST(JSON_EXTRACT_SCALAR(trafficByAsin, \'$.pageViewsPercentageB2B\') AS FLOAT64) AS pageViewsPercentageB2B FROM `asaya-data-room-487110.Maplemonk.Asaya_Amazon_BR_GET_SALES_AND_TRAFFIC_REPORT_ASIN`)",
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
            