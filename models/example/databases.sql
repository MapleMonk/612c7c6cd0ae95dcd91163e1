{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.sirona_child_level_Planning_FactItems as Select coalesce(EE.sku,INV.sku) SKU ,ORDER_Date ,PRODUCTNAME ,suborder_quantity Quantity ,selling_price ,Current_Inventory ,DDA ,ABC ,XYZ ,Zscore ,LT ,ROF from maplemonk.sirona_wh_EasyEcom_FACT_ITEMS EE left join( select sku, sum(CAST(Quantity as int64)) Current_Inventory, FROM `Maplemonk.Sirona_inv_db_consolidated_inventory` WHERE DATE(_airbyte_emitted_at) = CURRENT_DATE() AND Company_Name =\'SIRONA HYGIENE PRIVATE LIMITED(Tauru)\' group by 1) inv ON TRIM(UPPER(EE.SKU)) = TRIM(UPPER(INV.SKU)) left join( select sku ,CAST(DDA as float64) DDA ,ABC ,XYZ ,CAST(Zscore as float64) Zscore ,CAST(LT as int64) LT ,CAST(ROF as int64) ROF from `Maplemonk.Sirona_Google_sheet_db_Child_Level`) cl on UPPER(EE.SKU) = UPPER(CL.SKU) WHERE DATE(EE.order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY); CREATE OR REPLACE TABLE Maplemonk.sirona_db_emiza_planning_fact_items AS SELECT EE.sku AS SKU, EE.PRODUCTNAME AS PRODUCT_NAME, EE.ORDER_DATE, EE.selling_price AS SELLING_PRICE, EE.suborder_quantity AS SALES_QTY, EE.Warehouse_name AS WAREHOUSE_NAME, INV.Available_Stock FROM MAPLEMONK.sirona_wh_EasyEcom_FACT_ITEMS EE LEFT JOIN ( SELECT TRIM(UPPER(sku)) AS SKU, TRIM(UPPER(company_name)) AS COMPANY_NAME, SUM(CAST(Quantity AS INT64)) AS Available_Stock FROM `Maplemonk.Sirona_inv_db_consolidated_inventory` WHERE DATE(_airbyte_emitted_at) = CURRENT_DATE() GROUP BY 1,2 ) INV ON TRIM(UPPER(EE.SKU)) = INV.SKU AND TRIM(UPPER(EE.WAREHOUSE_NAME)) = INV.COMPANY_NAME WHERE UPPER(EE.WAREHOUSE_NAME) IN ( \'SIRONA HYGIENE PRIVATE LIMITED(EMZ - KARNATAKA)\', \'SIRONA HYGIENE PRIVATE LIMITED(TAURU)\', \'SIRONA HYGIENE PVT LTD(EMZ-HYD)\', \'SIRONA HYGIENE PVT LTD(EMZ-MUMBAI)\', \'SIRONA HYGIENE PRIVATE LIMITED(EMZ - WEST BENGAL)\' ) AND DATE(EE.order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) ;",
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
            