{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.sirona_child_level_Planning_FactItems as Select coalesce(EE.sku,INV.sku) SKU ,ORDER_Date ,PRODUCTNAME ,suborder_quantity Quantity ,selling_price ,Current_Inventory ,DDA ,ABC ,XYZ ,Zscore ,LT ,ROF from maplemonk.sirona_wh_EasyEcom_FACT_ITEMS EE left join( select sku, sum(CAST(Quantity as int64)) Current_Inventory, FROM `Maplemonk.Sirona_inv_db_consolidated_inventory` WHERE DATE(_airbyte_emitted_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND Company_Name =\'SIRONA HYGIENE PRIVATE LIMITED(Tauru)\' group by 1) inv ON TRIM(UPPER(EE.SKU)) = TRIM(UPPER(INV.SKU)) left join( select sku ,CAST(DDA as float64) DDA ,ABC ,XYZ ,CAST(Zscore as float64) Zscore ,CAST(LT as int64) LT ,CAST(ROF as int64) ROF from `Maplemonk.Sirona_Google_sheet_db_Child_Level`) cl on UPPER(EE.SKU) = UPPER(CL.SKU) WHERE DATE(EE.order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);",
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
            