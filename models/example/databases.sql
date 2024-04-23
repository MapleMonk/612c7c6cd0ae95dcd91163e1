{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.INVENTORY_PALN_V1 AS WITH category_inventory AS ( Select category AS Category_mapped, sum(QUANTITY) as Inventory from ( SELECT branch_name, CATEGORY, SUM(TOTAL_QTY) AS QUANTITY , current_date FROM ( SELECT *, CASE WHEN LOGICUSERCODE LIKE \'4MSN%\' THEN \'Shirt\' WHEN LOGICUSERCODE LIKE \'4MST%\' THEN \'T-Shirt\' WHEN LOGICUSERCODE LIKE \'4MBX%\' THEN \'Boxer\' WHEN LOGICUSERCODE LIKE \'4MSJ%\' THEN \'Jacket\' WHEN LOGICUSERCODE LIKE \'4MSNJ%\' THEN \'Jacket\' WHEN LOGICUSERCODE LIKE \'4MSW%\' THEN \'Co-Ords\' WHEN LOGICUSERCODE LIKE \'4MPJP%\' THEN \'Night Suit & Pyjamas\' WHEN LOGICUSERCODE LIKE \'4MTP%\' THEN \'Jogger\' WHEN LOGICUSERCODE LIKE \'4MPJ%\' THEN \'Night Suit & Pyjamas\' WHEN LOGICUSERCODE LIKE \'4MTR%\' THEN \'Chino\' WHEN LOGICUSERCODE LIKE \'4MSH%\' THEN \'Shorts\' WHEN LOGICUSERCODE LIKE \'4MSZ%\' THEN \'T-Shirt\' WHEN LOGICUSERCODE LIKE \'4MSNBC%\' THEN \'T-Shirt\' WHEN LOGICUSERCODE LIKE \'4MSNH%\' THEN \'Shorts\' WHEN LOGICUSERCODE LIKE \'4MSQ%\' THEN \'Shirt\' WHEN LOGICUSERCODE LIKE \'4MKJ%\' THEN \'Jogger\' WHEN LOGICUSERCODE LIKE \'4MSF%\' THEN \'Chino\' WHEN LOGICUSERCODE LIKE \'4MJK%\' THEN \'Jacket\' WHEN LOGICUSERCODE LIKE \'4MSS%\' THEN \'Shirt\' WHEN LOGICUSERCODE LIKE \'4MSWH%\' THEN \'Sweatshirt\' WHEN LOGICUSERCODE LIKE \'4MSCR%\' THEN \'Co-Ords\' WHEN LOGICUSERCODE LIKE \'4MSK%\' THEN \'Jacket\' WHEN LOGICUSERCODE LIKE \'4MSIWB%\' THEN \'Underpants\' WHEN LOGICUSERCODE LIKE \'4MSIWT%\' THEN \'Underpants\' WHEN LOGICUSERCODE LIKE \'4MSP%\' THEN \'Night Suit & Pyjamas\' WHEN LOGICUSERCODE LIKE \'4MSO%\' THEN \'Cargo\' WHEN LOGICUSERCODE LIKE \'4MCR%\' THEN \'Co-Ords\' WHEN LOGICUSERCODE LIKE \'4MSD%\' THEN \'Denim\' WHEN LOGICUSERCODE LIKE \'4MSC%\' THEN \'Chino\' WHEN LOGICUSERCODE LIKE \'4MSBX%\' THEN \'Boxer\' WHEN LOGICUSERCODE LIKE \'4MSR%\' THEN \'Trouser\' WHEN LOGICUSERCODE LIKE \'4MAMST%\' THEN \'T-Shirt\' WHEN LOGICUSERCODE LIKE \'4MAC%\' THEN \'Accessories\' WHEN LOGICUSERCODE LIKE \'SH%\' THEN \'Shoes\' WHEN LOGICUSERCODE LIKE \'SN%\' THEN \'Sunglass\' WHEN LOGICUSERCODE LIKE \'4MVK%\' THEN \'Denim\' WHEN LOGICUSERCODE LIKE \'4MBZ%\' THEN \'Blazer\' ELSE \'DEFAULT\' END AS Category, current_date FROM snitch_db.maplemonk.store_stock_aging WHERE CATEGORY != \'DEFAULT\' ) GROUP BY 1, 2, 4 UNION SELECT \'Online\' as branch_name, CASE WHEN category = \'Jogsuit\' THEN \'Night Suit & Pyjamas\' WHEN category = \'Pyjama\' THEN \'Night Suit & Pyjamas\' ELSE category END AS CATEGORY_MAPPED, COUNT(DISTINCT \"Item Code\") AS Inventory, current_date FROM SNITCH_DB.MAPLEMONK.unicommerce_inventory_aging_day_on_day WHERE _airbyte_emitted_at::DATE = DATEADD(DAY, -1, CURRENT_DATE()) GROUP BY 2,4 ) group by 1 ), category_SALES AS ( select abc.Mapped_Category as CATEGORY, sum(abc.Sales) as Revenue, sum(abc.Qty) as Qty, sum( abc.TOTAL_COGS) as COGS, DIV0(sum(abc.Sales)-sum( abc.TOTAL_COGS),sum(abc.Sales)) AS MARGIN FRom ( SELECT sales.CATEGORY, sales.SKU_GROUP, sales.Sales, sales.Qty, COALESCE(inventory_2.COGS,inventory.COGS_2,avg_cogs.avg_cogs) AS COGS, COALESCE(inventory_2.COGS,inventory.COGS_2,avg_cogs.avg_cogs)*sales.Qty AS TOTAL_COGS, DIV0(sales.Sales-COALESCE(inventory_2.COGS,inventory.COGS_2,avg_cogs.avg_cogs)*sales.Qty,sales.Sales) AS Margin, CASE sales.CATEGORY WHEN \'Gift Cards\' THEN \'Others\' WHEN \'Shoes\' THEN \'Shoes\' WHEN \'T-Shirt\' THEN \'T-Shirt\' WHEN \'SHORT\' THEN \'Shorts\' WHEN \'CATEGORY\' THEN \'Others\' WHEN \'Overshirt\' THEN \'Shirt\' WHEN \'Jacket\' THEN \'Jacket\' WHEN \'Cargo Pants\' THEN \'Cargo\' WHEN \'Chinos\' THEN \'Chino\' WHEN \'Denim\' THEN \'Denim\' WHEN \'INNER WEAR\' THEN \'Underpants\' WHEN \'(NIL)\' THEN \'Others\' WHEN \'SHOES REG DISC\' THEN \'Shoes\' WHEN \'CHINO\' THEN \'Chino\' WHEN \'Co-ords\' THEN \'Co-Ords\' WHEN \'Inner Wear\' THEN \'Underpants\' WHEN \'BLAZER\' THEN \'Blazer\' WHEN \'SHORTS\' THEN \'Shorts\' WHEN \'SWEATER\' THEN \'Sweater\' WHEN \'T-shirts\' THEN \'T-Shirt\' WHEN \'SHOES DISC\' THEN \'Shoes\' WHEN \'JOGGER\' THEN \'Jogger\' WHEN \'T-SHIRT\' THEN \'T-Shirt\' WHEN \'SHOES\' THEN \'Shoes\' WHEN \'PYJAMA\' THEN \'Night Suit & Pyjamas\' WHEN \'Perfumes\' THEN \'Perfumes\' WHEN \'Shirt\' THEN \'Shirt\' WHEN \'Pyjama\' THEN \'Night Suit & Pyjamas\' WHEN \'Jogger\' THEN \'Jogger\' WHEN \'Shirts\' THEN \'Shirt\' WHEN \'Trousers\' THEN \'Trouser\' WHEN \'CO-ORDS\' THEN \'Co-Ords\' WHEN \'Blazers\' THEN \'Blazer\' WHEN \'Sweaters\' THEN \'Sweater\' WHEN \'Accessories\' THEN \'Accessories\' WHEN \'Night Suit & Pyjamas\' THEN \'Night Suit & Pyjamas\' WHEN \'TROUSER\' THEN \'Trouser\' WHEN \'DENIM\' THEN \'Denim\' WHEN \'Hoodies\' THEN \'Sweatshirt\' WHEN \'null\' THEN \'Others\' WHEN \'JACKET\' THEN \'Jacket\' WHEN \'Sweatshirts\' THEN \'Sweatshirt\' WHEN \'Sunglasses\' THEN \'Sunglass\' WHEN \'PERFUME\' THEN \'Perfumes\' WHEN \'Jackets\' THEN \'Jacket\' WHEN \'SHIRT\' THEN \'Shirt\' WHEN \'Trouser\' THEN \'Trouser\' WHEN \'CARGO\' THEN \'Cargo\' WHEN \'T-Shirts\' THEN \'T-Shirt\' WHEN \'SUNGLASS\' THEN \'Sunglass\' WHEN \'Co-Ords\' THEN \'Co-Ords\' WHEN \'NA\' THEN \'Others\' WHEN \'Boxers\' THEN \'Boxer\' WHEN \'Joggers & Trackpants\' THEN \'Jogger\' WHEN \'SWEATSHIRT\' THEN \'Sweatshirt\' WHEN \'TRACK PANT\' THEN \'Night Suit & Pyjamas\' WHEN \'Boxer\' THEN \'Boxer\' WHEN \'Chino\' THEN \'Chino\' WHEN \'BOXER\' THEN \'Boxer\' WHEN \'SHIRTS\' THEN \'Shirt\' ELSE \'Other\' END AS Mapped_Category FROM ( SELECT CATEGORY, SKU_GROUP, LEFT(SKU_GROUP, 8) AS SKU_GROUP_2, LEFT(SKU_GROUP, 4) AS SKU_GROUP_3, SUM(SELLING_PRICE) AS Sales, SUM(COALESCE(SUBORDER_QUANTITY,SHIPPING_QUANTITY)) AS Qty FROM SNITCH_DB.MAPLEMONK.UNICOMMERCE_FACT_ITEMS_SNITCH WHERE ORDER_DATE >= DATEADD(DAY, -30, CURRENT_DATE()) AND ORDER_STATUS NOT IN (\'Cancelled\', \'CANCELLED\') GROUP BY 1, 2 ) AS sales LEFT JOIN ( SELECT DISTINCT LEFT(\"Item Type skuCode\", 8) AS sku_group_prefix, MAX(\"Unit price without tax\") AS COGS_2 FROM SNITCH_DB.MAPLEMONK.unicommerce_inventory_aging_day_on_day GROUP BY 1 ) AS inventory ON sales.SKU_GROUP_2 = inventory.sku_group_prefix LEFT JOIN ( SELECT DISTINCT(REVERSE(SUBSTRING(REVERSE(\"Item Type skuCode\"), CHARINDEX(\'-\', REVERSE(\"Item Type skuCode\")) + 1))) AS sku_group, MAX(\"Unit price without tax\") AS COGS FROM SNITCH_DB.MAPLEMONK.unicommerce_inventory_aging_day_on_day GROUP BY 1 ) AS inventory_2 ON sales.SKU_GROUP = inventory_2.sku_group LEFT JOIN ( SELECT DISTINCT LEFT(\"Item Type skuCode\", 4) AS sku_group_prefix_3, AVG(\"Unit price without tax\") AS avg_cogs FROM SNITCH_DB.MAPLEMONK.unicommerce_inventory_aging_day_on_day GROUP BY 1 ) AS avg_cogs ON sales.SKU_GROUP_3 = avg_cogs.sku_group_prefix_3) abc group by 1) SELECT ci.CATEGORY_MAPPED, ci.Inventory, SUM(S.Revenue) AS Revenue, SUM(S.Qty) as qty, SUM(S.COGS) as COGS, SUM(S.MARGIN) AS Margin, ROUND(DIV0(ci.Inventory, SUM(S.Qty)), 0) AS Inventory_on_hand_Month, ROUND(div0(ci.Inventory,SUM(S.Qty))*30,0) as DOI FROM category_inventory ci LEFT JOIN category_SALES s ON ci.CATEGORY_MAPPED = s.CATEGORY GROUP BY 1, 2; Create or Replace table snitch_db.maplemonk.one_inventory_view as with Inventory_View as ( SELECT branch_name, CATEGORY, sku_group, SUM(TOTAL_QTY) AS QUANTITY , current_date FROM ( SELECT *, REVERSE(SUBSTRING(REVERSE(LOGICUSERCODE), CHARINDEX(\'-\', REVERSE(LOGICUSERCODE)) + 1)) AS sku_group, CASE WHEN LOGICUSERCODE LIKE \'4MSN%\' THEN \'Shirt\' WHEN LOGICUSERCODE LIKE \'4MST%\' THEN \'T-Shirt\' WHEN LOGICUSERCODE LIKE \'4MBX%\' THEN \'Boxer\' WHEN LOGICUSERCODE LIKE \'4MSJ%\' THEN \'Jacket\' WHEN LOGICUSERCODE LIKE \'4MSNJ%\' THEN \'Jacket\' WHEN LOGICUSERCODE LIKE \'4MSW%\' THEN \'Co-Ords\' WHEN LOGICUSERCODE LIKE \'4MPJP%\' THEN \'Night Suit & Pyjamas\' WHEN LOGICUSERCODE LIKE \'4MTP%\' THEN \'Jogger\' WHEN LOGICUSERCODE LIKE \'4MPJ%\' THEN \'Night Suit & Pyjamas\' WHEN LOGICUSERCODE LIKE \'4MTR%\' THEN \'Chino\' WHEN LOGICUSERCODE LIKE \'4MSH%\' THEN \'Shorts\' WHEN LOGICUSERCODE LIKE \'4MSZ%\' THEN \'T-Shirt\' WHEN LOGICUSERCODE LIKE \'4MSNBC%\' THEN \'T-Shirt\' WHEN LOGICUSERCODE LIKE \'4MSNH%\' THEN \'Shorts\' WHEN LOGICUSERCODE LIKE \'4MSQ%\' THEN \'Shirt\' WHEN LOGICUSERCODE LIKE \'4MKJ%\' THEN \'Jogger\' WHEN LOGICUSERCODE LIKE \'4MSF%\' THEN \'Chino\' WHEN LOGICUSERCODE LIKE \'4MJK%\' THEN \'Jacket\' WHEN LOGICUSERCODE LIKE \'4MSS%\' THEN \'Shirt\' WHEN LOGICUSERCODE LIKE \'4MSWH%\' THEN \'Sweatshirt\' WHEN LOGICUSERCODE LIKE \'4MSCR%\' THEN \'Co-Ords\' WHEN LOGICUSERCODE LIKE \'4MSK%\' THEN \'Jacket\' WHEN LOGICUSERCODE LIKE \'4MSIWB%\' THEN \'Underpants\' WHEN LOGICUSERCODE LIKE \'4MSIWT%\' THEN \'Underpants\' WHEN LOGICUSERCODE LIKE \'4MSP%\' THEN \'Night Suit & Pyjamas\' WHEN LOGICUSERCODE LIKE \'4MSO%\' THEN \'Cargo\' WHEN LOGICUSERCODE LIKE \'4MCR%\' THEN \'Co-Ords\' WHEN LOGICUSERCODE LIKE \'4MSD%\' THEN \'Denim\' WHEN LOGICUSERCODE LIKE \'4MSC%\' THEN \'Chino\' WHEN LOGICUSERCODE LIKE \'4MSBX%\' THEN \'Boxer\' WHEN LOGICUSERCODE LIKE \'4MSR%\' THEN \'Trouser\' WHEN LOGICUSERCODE LIKE \'4MAMST%\' THEN \'T-Shirt\' WHEN LOGICUSERCODE LIKE \'4MAC%\' THEN \'Accessories\' WHEN LOGICUSERCODE LIKE \'SH%\' THEN \'Shoes\' WHEN LOGICUSERCODE LIKE \'SN%\' THEN \'Sunglass\' WHEN LOGICUSERCODE LIKE \'4MVK%\' THEN \'Denim\' WHEN LOGICUSERCODE LIKE \'4MBZ%\' THEN \'Blazer\' ELSE \'DEFAULT\' END AS Category, current_date FROM snitch_db.maplemonk.store_stock_aging WHERE CATEGORY != \'DEFAULT\' ) GROUP BY 1, 2,3 UNION SELECT \'Online\' as branch_name, REVERSE(SUBSTRING(REVERSE(\"Item Type skuCode\"), CHARINDEX(\'-\', REVERSE(\"Item Type skuCode\")) + 1)) AS sku_group, CASE WHEN category = \'Jogsuit\' THEN \'Night Suit & Pyjamas\' WHEN category = \'Pyjama\' THEN \'Night Suit & Pyjamas\' ELSE category END AS CATEGORY_MAPPED, COUNT(DISTINCT \"Item Code\") AS Inventory, current_date FROM SNITCH_DB.MAPLEMONK.unicommerce_inventory_aging_day_on_day WHERE _airbyte_emitted_at::DATE = DATEADD(DAY, -1, CURRENT_DATE()) GROUP BY 2, 3) SELECT ab.*, uam.sku_class FROM Inventory_View AS ab LEFT JOIN ( SELECT DISTINCT sku_group, sku_class, ROW_NUMBER() OVER (PARTITION BY sku_group ORDER BY 1) AS RN FROM snitch_db.maplemonk.availability_master ) AS uam ON ab.sku_group = uam.sku_group AND uam.RN = 1;",
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
                        