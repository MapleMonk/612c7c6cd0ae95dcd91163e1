{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace Table Maplemonk.BananaClub_Unicommerce_get_inventory_report as select MRP ,Size ,color ,shelf ,Style ,Facility ,Quantity ,SKU_Code ,Section ,Category ,REPLACE(LEFT(Ingested_At, 19), \'T\', \' \') AS Data_fetch_Date ,Inventory ,Base_Price ,Image_Link ,Description ,Product_Name ,Warehouse_Name from `MAPLEMONK.unicommerce_inventory_get_inventory_snapshot_not_found` QUALIFY ROW_NUMBER() OVER (PARTITION BY date(ingested_at),section,sku_code,facility,shelf ORDER BY REPLACE(LEFT(Ingested_At, 19), \'T\', \' \') DESC ) = 1;",
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
            