{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.BISMIL_COLLECTION AS with cost_price as ( select case when left(UPPER(TRIM(SKUGROUP)),2) = \'BP\' then split_part(UPPER(TRIM(SKUGROUP)),\'-\',0) else UPPER(TRIM(SKUGROUP)) end as sku_group, CATEGORY, max(cost_price) as cost_price, from snitch_db.maplemonk.uc_final_item_master where case when left(UPPER(TRIM(SKUGROUP)),2) = \'BP\' then split_part(UPPER(TRIM(SKUGROUP)),\'-\',0) else UPPER(TRIM(SKUGROUP)) end in (\'4MSK8754-01\', \'4MSD4120-01\',\'4MSD4120-02\',\'4MSD4121-01\',\'4MSS4355-01\',\'4MSS4355-02\',\'4MSS4356-01\',\'4MSS4357-01\',\'4MSS4358-01\', \'4MSS4359-01\',\'4MSS4360-01\',\'4MSS4361-01\',\'4MST3119-01\',\'4MST3120-01\',\'4MST3121-01\',\'4MST3122-01\',\'4MST3122-02\', \'4MSR5456-01\',\'4MSR5456-02\',\'4MSR5457-01\',\'4MSR5457-02\',\'4MSR5457-03\',\'4MSS4366-01\',\'4MSS4365-01\',\'4MHT1001-01\', \'4MCP1001-01\',\'4MCP1001-02\',\'4MSK8765-01\',\'4MOS1037-01\',\'4MBD0001-01\',\'4MBD0001-02\',\'4MBD0001-03\',\'4MBD0001-04\') group by 1,2 ), inv as ( select trim(upper(REVERSE(SUBSTRING(REVERSE(\"Item SkuCode\"), CHARINDEX(\'-\', REVERSE(\"Item SkuCode\")) + 1, LEN(\"Item SkuCode\"))))) AS sku_group, sum(inventory) as inventory from snitch_db.maplemonk.snitch_final_inventory_wh2 WHERE date = current_date and facility in (\'SAPL-WH2\',\'SAPL-WH1\',\'SAPL_EMIZA\',\'SAPL-NORTH-TAURU\') GROUP BY 1 union all select upper(trim(sku_group)) as sku_group, sum(inventory+jit_qty) as inventory from snitch_db.maplemonk.offline_master group by 1 ), INV_FINAL AS ( SELECT SKU_GROUP, IFNULL(SUM(inventory),0) AS INV FROM INV GROUP BY 1 ), SALES AS ( SELECT SKU_GROUP, SUM(GROSS_QUANTITY) AS SALES_QUANT, SUM(GROSS_SALES) AS SALES_POST, SUM(CASE WHEN GROSS_SALES <= 2499 THEN GROSS_SALES*0.95 ELSE GROSS_SALES*0.82 END) AS SALES_PRE, SUM(CASE WHEN DATE>=CURRENT_DATE-30 THEN GROSS_QUANTITY END) AS SALE_QUANT30 FROM SNITCH_DB.MAPLEMONK.HORIZONTAL_SALES_CATEGORIES GROUP BY 1 ), PRICE AS ( SELECT SKU_GROUP, COST_PRICE, ORIGINAL_PRICE FROM SNITCH_DB.MAPLEMONK.BISMIL_PRICE ) SELECT A.SKU_GROUP, A.CATEGORY, B.INV AS INV_COUNT, C.ORIGINAL_PRICE AS ASP, E.IMAGE, IFNULL(D.SALES_QUANT,0) AS SALES_QUANT, IFNULL(D.SALES_POST,0)::INT AS SALES_POST, IFNULL(D.SALES_PRE,0)::INT AS SALES_PRE, IFNULL(D.SALE_QUANT30,0) AS SALE_QUANT30, IFNULL(INV*C.COST_PRICE,0)::INT AS INV_VALUE, IFNULL(SALES_QUANT*C.COST_PRICE,0)::INT AS COGS, CASE WHEN IFNULL(SALE_QUANT30,0) = 0 THEN 10000 ELSE ((INV/SALE_QUANT30)*30)::INT END AS DOI FROM cost_price A LEFT JOIN INV_FINAL B ON A.SKU_GROUP = B.SKU_GROUP LEFT JOIN PRICE C ON A.SKU_GROUP = C.SKU_GROUP LEFT JOIN SALES D ON A.SKU_GROUP = D.SKU_GROUP LEFT JOIN snitch_db.maplemonk.product_journey E on A.SKU_GROUP = E.SKU_GROUP",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            