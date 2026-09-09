{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace Table Maplemonk.BananaClub_Unicommerce_get_inventory_report as select MRP ,Size ,color ,shelf ,Style ,Facility ,Cast(Quantity as int64) Quantity ,SKU_Code ,Section ,Category ,REPLACE(LEFT(Ingested_At, 19), \'T\', \' \') AS Data_fetch_Date ,Inventory ,Base_Price ,Image_Link ,Description ,Product_Name ,Warehouse_Name from `MAPLEMONK.unicommerce_inventory_get_inventory_snapshot_not_found` QUALIFY ROW_NUMBER() OVER (PARTITION BY date(ingested_at),section,sku_code,facility,shelf ORDER BY REPLACE(LEFT(Ingested_At, 19), \'T\', \' \') DESC ) = 1; CREATE OR REPLACE TABLE MAPLEMONK.BANANA_CLUB_INVENTORY_AVAILABILITY AS WITH SKU_MASTER AS ( SELECT DISTINCT SPLIT(PRODUCT_CODE, \'_\')[OFFSET(0)] AS SKU, UPPER(NAME) AS PRODUCT_NAME, UPPER(CATEGORY_NAME) AS CATEGORY, UPPER(DESCRIPTION) AS SUB_CATEGORY, UPPER(SIZE) AS SIZE, STYLE, COLOR AS STYLE_NO, IMAGE_URL AS IMAGE_LINK FROM `MAPLEMONK.BananaClub_DB_get_product_master` WHERE COLOR <> \'\' QUALIFY ROW_NUMBER() OVER ( PARTITION BY LOWER(TRIM(SPLIT(PRODUCT_CODE, \'_\')[OFFSET(0)])) ORDER BY 1 ) = 1 ), INVENTORY AS ( SELECT SKU_CODE, SUM(QUANTITY) AS QUANTITY FROM `Maplemonk.BananaClub_Unicommerce_get_inventory_report` WHERE DATE(DATA_FETCH_DATE) = CURRENT_DATE() AND UPPER(SECTION) = \'ONL\' GROUP BY 1 ) SELECT M.*, COALESCE(I.QUANTITY,0) QUANTITY FROM SKU_MASTER M LEFT JOIN INVENTORY I ON LOWER(TRIM(M.SKU)) = LOWER(TRIM(I.SKU_CODE));",
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
            