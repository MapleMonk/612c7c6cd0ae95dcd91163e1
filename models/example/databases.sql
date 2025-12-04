{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.TUCO_KIDS_FINAL_SKU_MASTER; CREATE TABLE public.TUCO_KIDS_FINAL_SKU_MASTER AS SELECT UPPER(REPLACE(CAST(size AS VARCHAR), \'\"\', \'\')) AS size, UPPER(CAST(color AS VARCHAR)) AS color, UPPER(REPLACE(CAST(title AS VARCHAR), \'\"\', \'\')) AS title, REPLACE(REPLACE(CAST(identifier AS VARCHAR), \'`\', \'\'), \'\'\'\', \'\') AS identifier, UPPER(REPLACE(REPLACE(CAST(\"Master sku\" AS VARCHAR), \'`\', \'\'), \'\'\'\', \'\')) AS master_sku, REPLACE(REPLACE(REPLACE(CAST(identifier1 AS VARCHAR), \'`\', \'\'), \'\'\'\', \'\'), \'-\', \'\') AS identifier1, REPLACE(REPLACE(REPLACE(CAST(identifier2 AS VARCHAR), \'`\', \'\'), \'\'\'\', \'\'), \'-\', \'\') AS identifier2, UPPER(CAST(marketplace AS VARCHAR)) AS marketplace, REPLACE(REPLACE(CAST(\"marketplace sku\" AS VARCHAR), \'`\', \'\'), \'\'\'\', \'\') AS marketplace_sku, NULL AS Category, NULL AS sub_category FROM public.tuco_kids_google_sheet_sku_master;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            