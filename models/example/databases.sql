{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace Table Maplemonk.IZF_Quick_Inventory_Fact_Items AS WITH ros_data AS ( SELECT marketplace, order_date, 100 AS current_inventory, Sold_Quantity, SUM(Sold_Quantity) OVER (PARTITION BY marketplace ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ros_7_days, SUM(Sold_Quantity) OVER (PARTITION BY marketplace ORDER BY order_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ros_10_days, SUM(Sold_Quantity) OVER (PARTITION BY marketplace ORDER BY order_date ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS ros_15_days FROM (SELECT marketplace, DATE(order_date) AS order_date, SUM(quantity) AS Sold_Quantity FROM `maplemonk.IZF_sales_consolidated` WHERE marketplace IN (\'KNOT NOW\', \'SLIKK\') GROUP BY marketplace, DATE(order_date)) ), po_data AS ( SELECT CASE WHEN pincode = \'400093\' THEN \'KNOT NOW\' WHEN pincode = \'560038\' THEN \'NEON MARKET\' ELSE marketplace END AS marketplace, DATE(TIMESTAMP(order_date)) AS po_date, COUNT(DISTINCT order_id) AS po_quantity FROM maplemonk.izf_easyecom_fact_items WHERE marketplace = \'B2B\' AND pincode IN (\'400093\', \'560038\') AND order_date IS NOT NULL GROUP BY marketplace, po_date ) SELECT r.order_date, r.marketplace, r.current_inventory, r.ros_7_days, r.ros_10_days, r.ros_15_days, p.po_date, COALESCE(p.po_quantity,0) AS po_quantity, r.current_inventory - COALESCE(r.ros_7_days,0) AS remaining_inventory_7d, r.current_inventory - COALESCE(r.ros_10_days,0) AS remaining_inventory_10d, r.current_inventory - COALESCE(r.ros_15_days,0) AS remaining_inventory_15d, (r.current_inventory - COALESCE(r.ros_7_days,0) + COALESCE(p.po_quantity,0)) AS total_inventory_7d, (r.current_inventory - COALESCE(r.ros_10_days,0) + COALESCE(p.po_quantity,0)) AS total_inventory_10d, (r.current_inventory - COALESCE(r.ros_15_days,0) + COALESCE(p.po_quantity,0)) AS total_inventory_15d FROM ros_data r LEFT JOIN po_data p ON UPPER(r.marketplace) = UPPER(p.marketplace) AND r.order_date = p.po_date ORDER BY r.order_date DESC;",
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
            