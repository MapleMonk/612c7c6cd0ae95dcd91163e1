{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.LIVE_INV_WAREHOUSE AS SELECT u.\"Item Type skuCode\", REVERSE(SUBSTRING(REVERSE(u.\"Item Type skuCode\"), CHARINDEX(\'-\', REVERSE(u.\"Item Type skuCode\")) + 1)) AS sku_group, COUNT(u.\"Item Type skuCode\") AS skuCodeCount, u.\"Item Type Name\", u.FACILITY, u.CATEGORY, u.\"Days in warehouse\", CASE WHEN u.\"Days in warehouse\" < 30 THEN \'0-30\' WHEN u.\"Days in warehouse\" >= 30 AND u.\"Days in warehouse\" < 60 THEN \'30-60\' WHEN u.\"Days in warehouse\" >= 60 AND u.\"Days in warehouse\" < 90 THEN \'60-90\' WHEN u.\"Days in warehouse\" >= 90 AND u.\"Days in warehouse\" < 120 THEN \'90-120\' WHEN u.\"Days in warehouse\" >= 120 AND u.\"Days in warehouse\" < 150 THEN \'120-150\' WHEN u.\"Days in warehouse\" >= 150 AND u.\"Days in warehouse\" < 180 THEN \'150-180\' WHEN u.\"Days in warehouse\" >= 180 AND u.\"Days in warehouse\" < 210 THEN \'180-210\' WHEN u.\"Days in warehouse\" >= 210 AND u.\"Days in warehouse\" < 240 THEN \'210-240\' WHEN u.\"Days in warehouse\" >= 240 AND u.\"Days in warehouse\" < 270 THEN \'240-270\' WHEN u.\"Days in warehouse\" >= 270 AND u.\"Days in warehouse\" < 300 THEN \'270-300\' WHEN u.\"Days in warehouse\" >= 300 AND u.\"Days in warehouse\" < 330 THEN \'300-330\' WHEN u.\"Days in warehouse\" >= 330 AND u.\"Days in warehouse\" < 360 THEN \'330-360\' ELSE \'> 360\' END AS days_in_warehouse_range, a.PRICE, a.FINAL_ROS, a.SALES_LAST_7_DAYS, a.SALES_LAST_15_DAYS, a.SALES_LAST_30_DAYS, a.SKU_CLASS FROM snitch_db.maplemonk.unicommerce_inventory_aging u LEFT JOIN SNITCH_DB.MAPLEMONK.AVAILABILITY_MASTER a ON LOWER(a.SKU_GROUP) = LOWER(REVERSE(SUBSTRING(REVERSE(u.\"Item Type skuCode\"), CHARINDEX(\'-\', REVERSE(u.\"Item Type skuCode\")) + 1))) GROUP BY u.\"Item Type skuCode\", u.\"Item Type Name\", u.FACILITY, u.CATEGORY, u.\"Days in warehouse\", a.sku_group, a.PRICE, a.FINAL_ROS, a.SALES_LAST_7_DAYS, a.SALES_LAST_15_DAYS, a.SALES_LAST_30_DAYS, a.SKU_CLASS; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.Outward_class_warehouse AS SELECT u.*, a.SKU_CLASS as sku_class123 FROM SNITCH_DB.maplemonk.unicommerce_fact_items_snitch u LEFT JOIN SNITCH_DB.maplemonk.availability_master a ON u.SKU_GROUP = a.SKU_GROUP; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.Outward_Inward_Difference AS SELECT A.DISPATCH_DATE, A.WAREHOUSE_NAME, COALESCE(A.OUTWARD_QUANTITY, 0) AS OUTWARD_QUANTITY, COALESCE(B.INWARD_QUANTITY, 0) AS INWARD_QUANTITY, CASE WHEN B.INWARD_QUANTITY = 0 THEN 0 WHEN B.INWARD_QUANTITY < 10 THEN 0 ELSE COALESCE(((B.INWARD_QUANTITY - A.OUTWARD_QUANTITY) / B.INWARD_QUANTITY) * 100, 0) END AS PERCENTAGE_DIFFERENCE FROM ( SELECT DISPATCH_DATE, WAREHOUSE_NAME, SUM(SHIPPING_QUANTITY) AS OUTWARD_QUANTITY FROM SNITCH_DB.MAPLEMONK.Outward_class_warehouse WHERE DISPATCH_DATE IS NOT NULL AND ORDER_STATUS=\'COMPLETE\' GROUP BY DISPATCH_DATE, WAREHOUSE_NAME ) AS A LEFT JOIN ( SELECT DATE, WAREHOUSE_NAME, SUM(QUANTITY) AS INWARD_QUANTITY FROM SNITCH_DB.maplemonk.sku_class_mapping GROUP BY DATE, WAREHOUSE_NAME ) AS B ON A.DISPATCH_DATE = B.DATE AND A.WAREHOUSE_NAME = B.WAREHOUSE_NAME ORDER BY A.DISPATCH_DATE DESC;",
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
                        