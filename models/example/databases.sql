{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; Create or replace table snitch_db.maplemonk.overall_aging as Select * , CASE WHEN sku_class IS NULL THEN \'Not in Shopify\' ELSE sku_class END AS Final_sku_class From ( With overall_inventory as( SELECT \"Item Type skuCode\" AS sku, REVERSE(SUBSTRING(REVERSE(REPLACE(\"Item Type skuCode\", \'\"\', \'\')), POSITION(\'-\' IN REVERSE(REPLACE(\"Item Type skuCode\", \'\"\', \'\'))) + 1)) AS sku_group, \"Item Type Name\" AS producT_name, case when \"Facility Id\" = 19614 then \'SAPL-EMIZA\' when \"Facility Id\" = 15129 then \'SAPL-WH\' when \"Facility Id\" = 16041 then \'SAPL-SR\' END AS FACILITY, CASE WHEN \"Days in warehouse\" < 30 THEN \'0-30\' WHEN \"Days in warehouse\" >= 30 AND \"Days in warehouse\" < 60 THEN \'30-60\' WHEN \"Days in warehouse\" >= 60 AND \"Days in warehouse\" < 90 THEN \'60-90\' WHEN \"Days in warehouse\" >= 90 AND \"Days in warehouse\" < 120 THEN \'90-120\' WHEN \"Days in warehouse\" >= 120 AND \"Days in warehouse\" < 150 THEN \'120-150\' WHEN \"Days in warehouse\" >= 150 AND \"Days in warehouse\" < 180 THEN \'150-180\' WHEN \"Days in warehouse\" >= 180 AND \"Days in warehouse\" < 210 THEN \'180-210\' WHEN \"Days in warehouse\" >= 210 AND \"Days in warehouse\" < 240 THEN \'210-240\' WHEN \"Days in warehouse\" >= 240 AND \"Days in warehouse\" < 270 THEN \'240-270\' WHEN \"Days in warehouse\" >= 270 AND \"Days in warehouse\" < 300 THEN \'270-300\' WHEN \"Days in warehouse\" >= 300 AND \"Days in warehouse\" < 330 THEN \'300-330\' WHEN \"Days in warehouse\" >= 330 AND \"Days in warehouse\" < 360 THEN \'330-360\' ELSE \'> 360\' END AS days_in_warehouse_range, \"Days in warehouse\", COUNT(DISTINCT \"Item Code\") AS Units_on_hand, category, CASE WHEN size = \'28\' THEN \'XS\' WHEN size = \'30\' THEN \'S\' WHEN size = \'32\' THEN \'M\' WHEN size = \'34\' THEN \'L\' WHEN size = \'36\' THEN \'XL\' WHEN size = \'38\' THEN \'XXL\' WHEN size = \'40\' THEN \'3XL\' WHEN size = \'42\' THEN \'4XL\' WHEN size = \'44\' THEN \'5XL\' WHEN size = \'46\' THEN \'6XL\' ELSE size END AS size_mapped, size FROM ( Select * FROM snitch_db.maplemonk.unicommerce_itembarcode_report WHERE \"Item Status\" in (\'GOOD_INVENTORY\',\'PUTBACK_PENDING\',\'PICKED\',\'NOT_FOUND\',\'PUTAWAY_PENDING\') AND \"Inventory type\"= \'GOOD_INVENTORY\') GROUP BY sku, sku_group, producT_name, FACILITY,\"Days in warehouse\", category, size UNION ALL Select sku, sku_group, product_name, facility, CASE WHEN WEIGHT_AGING < 30 THEN \'0-30\' WHEN WEIGHT_AGING >= 30 AND WEIGHT_AGING < 60 THEN \'30-60\' WHEN WEIGHT_AGING >= 60 AND WEIGHT_AGING < 90 THEN \'60-90\' WHEN WEIGHT_AGING >= 90 AND WEIGHT_AGING < 120 THEN \'90-120\' WHEN WEIGHT_AGING >= 120 AND WEIGHT_AGING < 150 THEN \'120-150\' WHEN WEIGHT_AGING >= 150 AND WEIGHT_AGING < 180 THEN \'150-180\' WHEN WEIGHT_AGING >= 180 AND WEIGHT_AGING < 210 THEN \'180-210\' WHEN WEIGHT_AGING >= 210 AND WEIGHT_AGING < 240 THEN \'210-240\' WHEN WEIGHT_AGING >= 240 AND WEIGHT_AGING < 270 THEN \'240-270\' WHEN WEIGHT_AGING >= 270 AND WEIGHT_AGING < 300 THEN \'270-300\' WHEN WEIGHT_AGING >= 300 AND WEIGHT_AGING < 330 THEN \'300-330\' WHEN WEIGHT_AGING >= 330 AND WEIGHT_AGING < 360 THEN \'330-360\' ELSE \'> 360\' END AS days_in_warehouse_range, WEIGHT_AGING, units_on_hand, category, size_mapped, size From ( wITH INVENT AS( Select \"Item SkuCode\" as sku, REVERSE(SUBSTRING(REVERSE(REPLACE(\"Item SkuCode\", \'\"\', \'\')), POSITION(\'-\' IN REVERSE(REPLACE(\"Item SkuCode\", \'\"\', \'\'))) + 1)) AS sku_group, \"Item Type Name\" AS producT_name, FACILITY, sum(inventory+ \"Inventory Blocked\"+\"Not Found\"+\"Putaway Pending\"+\"Putback Pending\") as Units_on_hand, \"Category Name\" as category, CASE WHEN size = \'28\' THEN \'XS\' WHEN size = \'30\' THEN \'S\' WHEN size = \'32\' THEN \'M\' WHEN size = \'34\' THEN \'L\' WHEN size = \'36\' THEN \'XL\' WHEN size = \'38\' THEN \'XXL\' WHEN size = \'40\' THEN \'3XL\' WHEN size = \'42\' THEN \'4XL\' WHEN size = \'44\' THEN \'5XL\' WHEN size = \'46\' THEN \'6XL\' ELSE size END AS size_mapped, size FROM snitch_db.maplemonk.snitch_final_inventory_wh2 WHERE date = current_date and inventory > 0 and CAST(facility AS VARCHAR) = \'SAPL-WH2\' group by sku, sku_group, producT_name, FACILITY, category, size ), AGIE AS ( Select * from ( WITH master AS ( SELECT \"Item SkuCode\" AS sku, SUM(inventory) AS units_on_hand, FROM snitch_db.maplemonk.snitch_final_inventory_wh2 WHERE date = CURRENT_DATE AND inventory > 0 GROUP BY sku ), PO AS ( SELECT \"Vendor SkuCode\" AS sku, \"Quantity Received\"::int AS qty, DATE_TRUNC(\'Day\', \"PO Date\"::date) AS created, DATEDIFF(DAY, DATE_TRUNC(\'Day\', \"GRN Date\"::date), CURRENT_DATE) AS aging FROM snitch_db.maplemonk.TEST_TO_DELETE_GET_GRN_REPORT WHERE facility = \'SAPL-WH2\' ), gatepass_cte AS ( SELECT DATE_TRUNC(\'Day\', \"Gatepass Created At\"::date) AS created, \"Item SkuCode\", \"Item Name\", SIZE, SUM(quantity) AS qty FROM snitch_db.maplemonk.unicommerce_gatepass WHERE \"To Party\" = \'YLK WH2\' AND \"Gatepass Order Code\" != \'TESTING1\' GROUP BY 1, 2, 3, 4 ), itembarcode_report_cte AS ( SELECT \"Item Type skuCode\" AS \"Item SkuCode\", ROUND(SUM(\"Days in warehouse\") / COUNT(*), 2) AS weighted_aging FROM snitch_db.maplemonk.unicommerce_itembarcode_report WHERE \"Facility Id\" = 15129 AND \"Item Status\" = \'LIQUIDATED\' GROUP BY \"Item Type skuCode\" ), GATEPASS AS ( SELECT g.created, g.\"Item SkuCode\", g.\"Item Name\", g.SIZE, g.qty, i.weighted_aging FROM gatepass_cte g LEFT JOIN itembarcode_report_cte i ON g.\"Item SkuCode\" = i.\"Item SkuCode\" ), MATCH AS ( SELECT a.sku, a.qty, a.created, CASE WHEN a.qty = b.qty THEN b.weighted_aging ELSE a.aging END AS final_aging, ROW_NUMBER() OVER (PARTITION BY a.sku ORDER BY a.created ASc) AS rn, FROM PO a LEFT JOIN GATEPASS b ON a.sku = b.\"Item SkuCode\" AND a.qty = b.qty ORDER BY rn DESC ), used_data AS ( Select sku , round(div0(sum(final_aging*FINAL_INVTORY),sum(FINAL_INVTORY)), 2) as weight_aging from ( select SKU, FINAL_AGING, CASE WHEN AGEING_QUANTITY<QTY THEN AGEING_QUANTITY ELSE QTY END AS FINAL_INVTORY FROM ( SELECT o.SKU, o.FINAL_AGING, o.QTY, o.RN, SUM(o.QTY) OVER ( PARTITION BY O.SKU ORDER BY O.RN DESC) AS CUM_QTY, p.units_on_hand as current_qty , case when CUM_QTY - O.QTY > current_qty THEN 0 ELSE current_qty - (CUM_QTY - O.QTY) END AS AGEING_QUANTITY, FROM MATCH o left join master p on o.sku =p.sku ) WHERE FINAL_INVTORY > 0 ) group by sku ) select * from used_data ) ) SELECT A.*, B.weight_aging FROM INVENT A LEFT JOIN AGIE B ON A.SKU=B.SKU ) ), class as ( SELECT SKU_GROUP , SKU_CLASS, FINAL_ROS, NATURAL_ROS FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY SKU_GROUP ORDER BY 1) AS RN FROM SNITCH_DB.MAPLEMONK.availability_master_v2 ) where RN = 1 ) Select a.*, b.sku_class, b.final_ros, b.natural_ros from overall_inventory a left join class b on a.sku_group = b.sku_group ) ; create or replace table snitch_db.maplemonk.Inwards as WITH DerivedTable AS ( SELECT CASE WHEN RIGHT(a.\"Item Type skuCode\", 2) = \'-S\' THEN LEFT(a.\"Item Type skuCode\", LEN(a.\"Item Type skuCode\") - 2) ELSE REPLACE(a.\"Item Type skuCode\", CONCAT(\'-\', SPLIT_PART(a.\"Item Type skuCode\", \'-\', -1)), \'\') END AS sku_group, LEFT(a.\"UPDATED\", 10)::DATE AS date, case when _AB_SOURCE_FILE_URL like \'%SAPL-SR%\' then \'SAPL-SR\' when _AB_SOURCE_FILE_URL like \'%SAPL-EMIZA%\' then \'SAPL-EMIZA\' when _AB_SOURCE_FILE_URL like \'%SAPL-WH2%\' then \'SAPL-WH2\' else \'SAPL-WH\' end warehouse_name, SUM( case when type = \'PUTAWAY_GRN_ITEM\' then a.\"Putaway Quantity\" else 0 end) AS Inwared_quantity, SUM( case when type != \'PUTAWAY_GRN_ITEM\' then a.\"Putaway Quantity\" else 0 end) AS Return_quantity, a.category FROM snitch_db.maplemonk.unicommerce_putaway_report a group by 1,2,3, a.category ) SELECT dt.sku_group, dt.date, dt.Inwared_quantity, dt.Return_quantity, dt.warehouse_name, dt.category, case when b.sku_class is null then \'4-New\' else b.sku_class end Sku_class, b.final_ros, b.product_name FROM DerivedTable dt LEFT JOIN snitch_db.maplemonk.availability_master_v2 b ON dt.sku_group = b.sku_group; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.liquidation_list AS SELECT ia.SKU, ia.SKU_GROUP, ia.PRODUCT_NAME, ia.DAYS_IN_WAREHOUSE_RANGE, SUM(ia.UNITS_ON_HAND) AS TOTAL_UNITS_ON_HAND, ia.CATEGORY, ia.size_mapped, ia.facility, ol.Last_7_days_sales, r.FINAL_ROS FROM ( SELECT SKU, SKU_GROUP, PRODUCT_NAME, DAYS_IN_WAREHOUSE_RANGE, SUM(UNITS_ON_HAND) AS UNITS_ON_HAND, CATEGORY, facility, size_mapped, FROM Snitch_db.maplemonk.overall_aging WHERE DAYS_IN_WAREHOUSE_RANGE NOT IN (\'61-90\', \'91-120\', \'31-60\', \'0-30\') GROUP BY SKU, SKU_GROUP, PRODUCT_NAME, DAYS_IN_WAREHOUSE_RANGE, CATEGORY, facility, size_mapped ) AS ia LEFT JOIN ( SELECT SKU, SKU_GROUP, AVG(SHIPPING_QUANTITY) AS Last_7_days_sales FROM snitch_db.snitch.order_lineitems_fact WHERE order_date::date > (SELECT MAX(order_date::date) FROM snitch_db.snitch.order_lineitems_fact) - 7 GROUP BY SKU, SKU_GROUP ) AS ol ON ia.SKU = ol.SKU AND ia.SKU_GROUP = ol.SKU_GROUP LEFT JOIN ( SELECT SKU_GROUP, FINAL_ROS FROM SNITCH_DB.MAPLEMONK.ros_snitch ) AS r ON ia.SKU_GROUP = r.SKU_GROUP GROUP BY ia.SKU, ia.SKU_GROUP, ia.PRODUCT_NAME, ia.DAYS_IN_WAREHOUSE_RANGE, ia.CATEGORY, ia.size_mapped, ia.facility, ol.Last_7_days_sales, r.FINAL_ROS ; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.Outward_Inward_Difference AS SELECT A.DISPATCH_DATE, A.WAREHOUSE_NAME, COALESCE(A.OUTWARD_QUANTITY, 0) AS OUTWARD_QUANTITY, COALESCE(B.INWARD_QUANTITY, 0) AS INWARD_QUANTITY, CASE WHEN B.INWARD_QUANTITY = 0 THEN 0 WHEN B.INWARD_QUANTITY < 10 THEN 0 ELSE COALESCE(((B.INWARD_QUANTITY - A.OUTWARD_QUANTITY) / B.INWARD_QUANTITY) * 100, 0) END AS PERCENTAGE_DIFFERENCE FROM ( SELECT DISPATCH_DATE, WAREHOUSE_NAME, SUM(SHIPPING_QUANTITY) AS OUTWARD_QUANTITY FROM SNITCH_DB.MAPLEMONK.Outward_class_warehouse WHERE DISPATCH_DATE IS NOT NULL AND ORDER_STATUS=\'COMPLETE\' GROUP BY DISPATCH_DATE, WAREHOUSE_NAME ) AS A LEFT JOIN ( SELECT DATE, WAREHOUSE_NAME, SUM(QUANTITY) AS INWARD_QUANTITY FROM SNITCH_DB.maplemonk.sku_class_mapping GROUP BY DATE, WAREHOUSE_NAME ) AS B ON A.DISPATCH_DATE = B.DATE AND A.WAREHOUSE_NAME = B.WAREHOUSE_NAME ORDER BY A.DISPATCH_DATE DESC; create or replace table snitch_db.maplemonk.P90 AS select PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY DATEDIFF(\'day\', order_date, delivered_date)) AS p90_days_to_delivery_order, PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY DATEDIFF(\'day\', dispatch_date, delivered_date)) AS p90_days_to_delivery_dispatch, warehouse_name, ZONE from( select b.*, a.zone FROM snitch_db.maplemonk.unicommerce_fact_items_snitch b left join (select zone,delivery_state,city_tier,delivery_postcode from( select *, ROW_NUMBER() OVER (PARTITION BY DELIVERY_POSTCODE ORDER BY 1) RN from snitch_db.maplemonk.pincodemappingzoneupdatedsnitch) where rn=1) a on b.pincode =a.DELIVERY_POSTCODE) WHERE lower(marketplace_mapped) = \'shopify\' AND order_status = \'COMPLETE\' GROUP BY warehouse_name, Zone; Create or replace Table snitch_db.maplemonk.Shipment_ratio AS SELECT DISTINCT QTY , COUNT (AWB) as Total_Shipments FROM ( SELECT DISTINCT AWB, SUM(SHIPPING_QUANTITY) AS qty, FROM snitch_db.maplemonk.UNICOMMERCE_FACT_ITEMS_SNITCH WHERE ORDER_STATUS = \'COMPLETE\' AND MARKETPLACE_MAPPED = \'SHOPIFY\' and order_date >= getdate()::date - 30 and awb is not null GROUP BY 1 ) GROUP BY 1 ORDER BY QTY DESC;",
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
            