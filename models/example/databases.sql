{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Myntra_Final_Inventory AS SELECT * FROM ( SELECT PARSE_DATE(\'%d-%m-%Y\', i.Date) AS DATA_FETCH_DATE, CAST(i.style_id AS STRING) AS Style_Id, CAST(m.Myntra AS STRING) AS Myntra, CAST(m.WMS_SKU AS STRING) AS COMMONSKU, CAST(m.Category AS STRING) AS Category, CAST(i.inv_units_q1 AS INT64) AS inv_units_q1, CAST(SUM(COALESCE(s.Quantity, 0)) AS INT64) AS SOLD_QTY_30D, CAST(NULL AS DATE) AS Order_Date, CAST(NULL AS INT64) AS Quantity, CAST(\'INVENTORY\' AS STRING) AS RECORD_TYPE FROM MapleMonk.ZOUK_MYNTRA_INVENTORY AS i LEFT JOIN Maplemonk.SKU_Mapping AS m ON i.style_id = m.Myntra LEFT JOIN MapleMonk.zouk_Secondary_sales_consolidated AS s ON s.CommonSKU = m.WMS_SKU AND s.Marketplace = \'MYNTRAPPMP\' AND DATE_DIFF(PARSE_DATE(\'%d-%m-%Y\', i.Date), s.Order_Date, DAY) BETWEEN 1 AND 30 GROUP BY i.Date, i.style_id, m.Myntra, m.WMS_SKU, m.Category, i.inv_units_q1 UNION ALL SELECT CAST(NULL AS DATE) AS DATA_FETCH_DATE, CAST(NULL AS STRING) AS Style_Id, CAST(NULL AS STRING) AS Myntra, CAST(m.WMS_SKU AS STRING) AS COMMONSKU, CAST(NULL AS STRING) AS Category, CAST(NULL AS INT64) AS inv_units_q1, CAST(NULL AS INT64) AS SOLD_QTY_30D, s.Order_Date AS Order_Date, CAST(s.Quantity AS INT64) AS Quantity, CAST(\'SALES\' AS STRING) AS RECORD_TYPE FROM MapleMonk.zouk_Secondary_sales_consolidated AS s LEFT JOIN Maplemonk.SKU_Mapping AS m ON s.CommonSKU = m.WMS_SKU WHERE s.Marketplace = \'MYNTRAPPMP\' ) ORDER BY DATA_FETCH_DATE DESC, Order_Date DESC;",
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
            