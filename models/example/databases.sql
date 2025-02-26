{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table ras-wh.maplemonk.ras_inventory_fact_items as select upper(productSKU) SKU ,coalesce(sm.product_name, upper(productName)) Product_Name ,sm.brand ,sm.category ,sm.mrp ,upper(batch) Batch ,upper(binName) binName ,cast(shelfLife as int64) Shelf_Life ,parse_date(\'%d-%m-%Y\',manufacturingDate) Manufacturing_date ,parse_date(\'%d-%m-%Y\',expiryDate) Expiry_Date ,DATE_DIFF( PARSE_DATE(\'%d-%m-%Y\', expiryDate), PARSE_DATE(\'%d-%m-%Y\', manufacturingDate), DAY ) AS Estimated_Shelf_Life_Days ,DATE_DIFF( PARSE_DATE(\'%d-%m-%Y\', expiryDate), PARSE_DATE(\'%d-%m-%Y\', manufacturingDate), MONTH ) AS Estimated_Shelf_Life_Months ,DATE_DIFF( PARSE_DATE(\'%d-%m-%Y\', expiryDate), CURRENT_DATE(\'Asia/Kolkata\'), DAY ) AS Remaining_Life_Days ,DATE_DIFF( PARSE_DATE(\'%d-%m-%Y\', expiryDate), CURRENT_DATE(\'Asia/Kolkata\'), MONTH ) AS Remaining_Life_Months ,case when DATE_DIFF( PARSE_DATE(\'%d-%m-%Y\', expiryDate), CURRENT_DATE(\'Asia/Kolkata\'), DAY ) <=0 then \'EXPIRED\' else \'ACTIVE\' end Expiry_Status ,case when DATE_DIFF( PARSE_DATE(\'%d-%m-%Y\', expiryDate), CURRENT_DATE(\'Asia/Kolkata\'), DAY ) > 365 then \'MORE THAN A YEAR\' when DATE_DIFF( PARSE_DATE(\'%d-%m-%Y\', expiryDate), CURRENT_DATE(\'Asia/Kolkata\'), DAY ) > 270 then \'271 DAYS - 1 YEAR\' when DATE_DIFF( PARSE_DATE(\'%d-%m-%Y\', expiryDate), CURRENT_DATE(\'Asia/Kolkata\'), DAY ) > 180 then \'181 - 270 DAYS\' when DATE_DIFF( PARSE_DATE(\'%d-%m-%Y\', expiryDate), CURRENT_DATE(\'Asia/Kolkata\'), DAY ) > 90 then \'91 - 180 DAYS\' when DATE_DIFF( PARSE_DATE(\'%d-%m-%Y\', expiryDate), CURRENT_DATE(\'Asia/Kolkata\'), DAY ) > 60 then \'61 - 90 DAYS\' when DATE_DIFF( PARSE_DATE(\'%d-%m-%Y\', expiryDate), CURRENT_DATE(\'Asia/Kolkata\'), DAY ) > 30 then \'31 - 60 DAYS\' when DATE_DIFF( PARSE_DATE(\'%d-%m-%Y\', expiryDate), CURRENT_DATE(\'Asia/Kolkata\'), DAY ) > 15 then \'16 - 30 DAYS\' else \'LESS THAN 15 DAYS\' end REMAINING_DAYS_CATEGORY ,cast(availableQuantity as int64) Available_Quantity from `MAPLEMONK.edgistify_get_master_inventory_details` id left join ras-wh.maplemonk.ras_sku_master sm on upper(id.productSKU) = sm.sku ;",
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
            