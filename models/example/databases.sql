{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table jpearls_db.maplemonk.SKU_MASTER as select \"Product Code\" Product_Code ,\"Scan Identifier\" Scan_Identifier ,\"HSN CODE\" HSN_CODE ,upper(\"Category Name\") Category ,\"Image Url\" Image_URL ,upper(\"COLOR\") Color ,upper(SIZE) SIZE ,Upper(NAME) Product_Name ,try_cast(replace(MRP,\'\"\',\'\') as float) MRP ,try_cast(replace(\"Cost Price\",\'\"\',\'\') as float) COST_PRICE ,try_cast(replace(\"Base Price\",\'\"\',\'\') as float) BASE_PRICE ,try_cast(replace(\"Weight (gms)\",\'\"\',\'\') as float) WEIGHT from jpearls_db.maplemonk.UNICOMMERCE_JPEARLS_UNICOMMERCE_GET_PRODUCT_MASTER;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from JPEARLS_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        