{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.matrixstory_flipkart_inventory as SELECT UPPER(CAST(f.warehouse_id AS STRING)) AS location, DATE(TIMESTAMP(SUBSTR(created_at,1,19))) - 1 AS data_fetch_date, CAST(NULL AS STRING) AS company_token, REPLACE( UPPER(REPLACE(CAST(fsn AS STRING), \'\"\', \'\')), \' \', \'\' ) AS product_id, CAST(title AS STRING) AS product_name, CAST(NULL AS INT64) AS repair, CAST(damaged AS INT64) AS damaged, CAST(NULL AS INT64) AS received, CAST(Reserved_for_Orders_and_Recalls AS INT64) + CAST(Reserved_for_Internal_Processing AS INT64) AS reserved, CAST(qc_reject AS INT64) AS QC_Failed, CAST(NULL AS INT64) AS QC_Passed, CAST(NULL AS INT64) AS QC_Pending, CAST(NULL AS INT64) AS Total_Lost, CAST(NULL AS INT64) AS discard_fraud, CAST(live_on_website AS INT64) AS Available_Quantity, CAST(recalls_to_dispatch AS INT64) AS Undispatched_Unassigned_Quantity, ROW_NUMBER() OVER ( PARTITION BY UPPER(REPLACE(fsn, \' \', \'\')), f.warehouse_id, DATE(TIMESTAMP(SUBSTR(created_at,1,19))) ORDER BY TIMESTAMP(_airbyte_normalized_at) DESC, TIMESTAMP(SUBSTR(created_at,1,19)) DESC ) AS rw FROM `maplemonk.MatrixStore_Flipkart_current_inventory_report` f ;",
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
            