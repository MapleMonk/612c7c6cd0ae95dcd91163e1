{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `maplemonk.Buildskill_S3_Amazon_Daily_Sales_Report_Final` AS SELECT _airbyte_ab_id, _airbyte_emitted_at, SAFE.PARSE_DATE(\'%m-%d-%Y\', NULLIF(JSON_VALUE(_airbyte_data, \'$.\"Sales Date\"\'), \'\') ) AS sales_date, JSON_VALUE(_airbyte_data, \'$.ASIN\') AS asin, JSON_VALUE(_airbyte_data, \'$.\"Product Title\"\') AS product_title, JSON_VALUE(_airbyte_data, \'$.Brand\') AS brand, SAFE_CAST(NULLIF(JSON_VALUE(_airbyte_data, \'$.\"Ordered Units\"\'), \'\') AS INT64) AS ordered_units, SAFE_CAST(NULLIF(JSON_VALUE(_airbyte_data, \'$.\"Shipped Units\"\'), \'\') AS INT64) AS shipped_units, SAFE_CAST(NULLIF(JSON_VALUE(_airbyte_data, \'$.\"Ordered Revenue\"\'), \'\') AS NUMERIC) AS ordered_revenue, SAFE_CAST(NULLIF(JSON_VALUE(_airbyte_data, \'$.\"Shipped Revenue\"\'), \'\') AS NUMERIC) AS shipped_revenue, SAFE_CAST(NULLIF(JSON_VALUE(_airbyte_data, \'$.\"Shipped COGS\"\'), \'\') AS NUMERIC) AS shipped_cogs, SAFE_CAST(NULLIF(JSON_VALUE(_airbyte_data, \'$.\"Customer Returns\"\'), \'\') AS INT64) AS customer_returns, SAFE_CAST( NULLIF(JSON_VALUE(_airbyte_data, \'$._ab_source_file_last_modified\'), \'\') AS TIMESTAMP ) AS source_file_last_modified, JSON_VALUE(_airbyte_data, \'$._ab_source_file_url\') AS source_file_url, _airbyte_data FROM `maplemonk._airbyte_raw_Buildskill_S3_Amazon_Daily_Sales_Report`;",
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
            