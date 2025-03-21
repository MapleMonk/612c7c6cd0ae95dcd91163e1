{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MapleMonk.Zouk_Unicommerce_Warehouse_Level_Inventory` AS SELECT * FROM ( SELECT DATETIME(TIMESTAMP(timestamp), \"Asia/Kolkata\") AS DATA_FETCH_DATE, REPLACE(SKUCODE, \'\\'\', \'\') AS SKU, FACILITY AS LOCATION, INVENTORY AS AVAILABLE_INVENTORY, ROW_NUMBER() OVER (PARTITION BY FACILITY, REPLACE(SKUCODE, \'\\'\', \'\'), cast(DATETIME(TIMESTAMP(timestamp), \"Asia/Kolkata\") as date) ORDER BY DATETIME(TIMESTAMP(timestamp), \"Asia/Kolkata\") DESC) AS rw FROM MAPLEMONK.UNICOMMERCE_ZOUK_UC_GET_INVENTORY_SNAPSHOT ) WHERE rw = 1;",
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
            