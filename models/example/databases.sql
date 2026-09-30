{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.SleepyCat_flipkart_inventory AS SELECT UPPER(CAST(f.\"Warehouse Id\" AS VARCHAR)) AS location, TO_DATE( TO_TIMESTAMP(SUBSTR(created_at, 1, 19)) ) - 1 AS data_fetch_date, CAST(NULL AS VARCHAR) AS company_token, REPLACE( UPPER(REPLACE(CAST(fsn AS VARCHAR), \'\"\', \'\')), \' \', \'\' ) AS product_id, CAST(title AS VARCHAR) AS product_name, CAST(NULL AS INTEGER) AS repair, CAST(damaged AS INTEGER) AS damaged, CAST(NULL AS INTEGER) AS received, CAST(\"Reserved for Orders and Recalls\" AS INTEGER) + CAST(\"Reserved for Internal Processing\" AS INTEGER) AS reserved, CAST(\"QC Reject\" AS INTEGER) AS QC_Failed, CAST(NULL AS INTEGER) AS QC_Passed, CAST(NULL AS INTEGER) AS QC_Pending, CAST(NULL AS INTEGER) AS Total_Lost, CAST(NULL AS INTEGER) AS discard_fraud, CAST(\"Live on Website\" AS INTEGER) AS Available_Quantity, CAST(\"Recalls to Dispatch\" AS INTEGER) AS Undispatched_Unassigned_Quantity, ROW_NUMBER() OVER ( PARTITION BY UPPER(REPLACE(fsn, \' \', \'\')), f.\"Warehouse Id\", TO_DATE(TO_TIMESTAMP(SUBSTR(created_at, 1, 19))) ORDER BY TO_TIMESTAMP(_airbyte_normalized_at) DESC, TO_TIMESTAMP(SUBSTR(created_at, 1, 19)) DESC ) AS rw FROM maplemonk.Sleepycat_db_current_inventory_report f;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SLEEPYCAT_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            