{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.sirona_db_Consolidated_inventory_FactItems as WITH base AS ( SELECT *, TIMESTAMP_TRUNC(_airbyte_emitted_at, HOUR) AS run_hour FROM `Maplemonk.Sirona_inv_db_consolidated_inventory` WHERE Company_Name =\'SIRONA HYGIENE PRIVATE LIMITED(Tauru)\' ), latest_run AS ( SELECT DATE(_airbyte_emitted_at) AS Date, MAX(run_hour) AS max_run_hour FROM base GROUP BY 1 ) SELECT CAST(FORMAT_TIMESTAMP(\'%Y-%m-%d\', a._airbyte_emitted_at) AS DATE) AS Date, a._airbyte_emitted_at, mrp, a.sku, zone, shelf, EAN_No, Status, CAST(Quantity as int64) Quantity, Serial_No, Brand_Name, Expiry_Date, Company_Name, COALESCE(b.Sku_Name,a.Product_Name) Product_Name, Days_to_Expire, Manufacturing_Date, Shelf_Life_Percentage, COALESCE(CAST(REPLACE(b.Cogs_CURRENT_SHEET, \',\', \'\') AS FLOAT64),0) AS cogs, CAST(b.DDA__FMA_ AS FLOAT64) DDA, CASE WHEN a.Shelf_Life_Percentage IS NULL OR TRIM(a.Shelf_Life_Percentage) = \'\' OR a.Shelf_Life_Percentage IN (\'NA\',\'N/A\',\'-\') OR CAST(a.Shelf_Life_Percentage AS FLOAT64) = 0 THEN \'No Expiry\' WHEN CAST(a.Shelf_Life_Percentage AS FLOAT64) < 40 THEN \'Less than 40%\' WHEN CAST(a.Shelf_Life_Percentage AS FLOAT64) < 55 THEN \'40%<x<55%\' WHEN CAST(a.Shelf_Life_Percentage AS FLOAT64) < 70 THEN \'55%<x<70%\' WHEN CAST(a.Shelf_Life_Percentage AS FLOAT64) > 70 THEN \'Greater than 70%\' END AS Ageing, single_type, product_type, CASE WHEN COUNT(*) OVER (PARTITION BY a.sku) > COUNT(*) OVER (PARTITION BY a.sku, a.mrp) THEN TRUE ELSE FALSE END AS mrp_double FROM base a LEFT JOIN maplemonk.sirona_gs_db_master_sheet b ON a.sku = b.sku JOIN latest_run l ON DATE(a._airbyte_emitted_at) = l.Date AND a.run_hour = l.max_run_hour; CREATE OR REPLACE TABLE maplemonk.sirona_db_Consolidated_Marketplace_FactItems AS WITH base AS ( SELECT *, TIMESTAMP_TRUNC(_airbyte_emitted_at, HOUR) AS run_hour FROM `Maplemonk.Sirona_inv_db_consolidated_inventory` ), latest_run AS ( SELECT DATE(_airbyte_emitted_at) AS Date, MAX(run_hour) AS max_run_hour FROM base GROUP BY 1 ) SELECT FORMAT_TIMESTAMP(\'%Y-%m-%d\', a._airbyte_emitted_at) AS Date, mrp, sku, zone, shelf, EAN_No, Status, CAST(Quantity as int64) Quantity, Serial_No, Brand_Name, Expiry_Date, Company_Name, Product_Name, Days_to_Expire, Manufacturing_Date, Shelf_Life_Percentage FROM base a JOIN latest_run l ON DATE(a._airbyte_emitted_at) = l.Date AND a.run_hour = l.max_run_hour;",
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
            