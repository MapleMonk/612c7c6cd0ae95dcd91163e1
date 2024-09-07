{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.ROS_Snitch as WITH sku_group_data AS ( SELECT REVERSE(SUBSTRING(REVERSE(sku), CHARINDEX(\'-\', REVERSE(sku)) + 1)) AS sku_group, order_date, suborder_quantity, return_quantity FROM snitch_db.MAPLEMONK.UNICOMMERCE_FACT_ITEMS_SNITCH WHERE marketplace_mapped not in (\'OWN_STORE\',\'FRANCHISE_STORE\') and marketplace_mapped not like \'%WH%\' AND REVERSE(SUBSTRING(REVERSE(sku), CHARINDEX(\'-\', REVERSE(sku)) + 1)) NOT LIKE\'CB%\' ), first_order_dates AS ( SELECT sku_group, MIN(order_date) AS first_order_date, DATEDIFF(\'DAY\', MIN(order_date), CURRENT_DATE) AS days_since_first_order, DATEDIFF(\'DAY\', MIN(order_date), MAX(order_date)) AS total_days_sold FROM sku_group_data GROUP BY sku_group ), total_sales AS ( SELECT sku_group_data.sku_group, first_order_dates.first_order_date, first_order_dates.days_since_first_order, first_order_dates.total_days_sold, SUM(return_quantity) AS total_returns, SUM(suborder_quantity) AS total_sales, SUM(CASE WHEN order_date <= DATEADD(\'DAY\', 30, first_order_date) THEN suborder_quantity ELSE 0 END) AS sales_first_30_days, SUM(CASE WHEN order_date <= DATEADD(\'DAY\', 15, first_order_date) THEN suborder_quantity ELSE 0 END) AS sales_first_15_days, SUM(CASE WHEN order_date <= DATEADD(\'DAY\', 7, first_order_date) THEN suborder_quantity ELSE 0 END) AS sales_first_7_days, SUM(CASE WHEN order_date <= DATEADD(\'DAY\', 90, first_order_date) THEN suborder_quantity ELSE 0 END) AS sales_first_90_days, SUM(CASE WHEN order_date <= DATEADD(\'DAY\', 60, first_order_date) THEN suborder_quantity ELSE 0 END) AS sales_first_60_days, SUM(CASE WHEN order_date <= DATEADD(\'DAY\', 180, first_order_date) THEN suborder_quantity ELSE 0 END) AS sales_first_180_days, SUM(CASE WHEN CURRENT_DATE - order_date <= 30 THEN suborder_quantity ELSE 0 END) AS sales_last_30_days, SUM(CASE WHEN CURRENT_DATE - order_date <= 15 THEN suborder_quantity ELSE 0 END) AS sales_last_15_days, SUM(CASE WHEN CURRENT_DATE - order_date <= 7 THEN suborder_quantity ELSE 0 END) AS sales_last_7_days, SUM(CASE WHEN CURRENT_DATE - order_date <= 60 THEN suborder_quantity ELSE 0 END) AS sales_last_60_days, SUM(CASE WHEN CURRENT_DATE - order_date <= 90 THEN suborder_quantity ELSE 0 END) AS sales_last_90_days, SUM(CASE WHEN CURRENT_DATE - order_date <= 180 THEN suborder_quantity ELSE 0 END) AS sales_last_180_days, CASE WHEN first_order_dates.total_days_sold = 0 THEN SUM(suborder_quantity) ELSE SUM(suborder_quantity) / first_order_dates.total_days_sold END AS natural_ros FROM sku_group_data INNER JOIN first_order_dates ON sku_group_data.sku_group = first_order_dates.sku_group GROUP BY sku_group_data.sku_group, first_order_dates.first_order_date, first_order_dates.days_since_first_order, first_order_dates.total_days_sold ), enhanced_total_sales AS ( SELECT total_sales.*, GREATEST( total_sales.sales_first_30_days / 30, total_sales.sales_last_30_days / 30, total_sales.natural_ros ) AS max_first_30_last_30_natural_ros, GREATEST( total_sales.sales_first_30_days / 30, total_sales.sales_last_30_days / 30, total_sales.natural_ros, total_sales.sales_first_7_days / 7 ) AS max_first_30_last_30_natural_ros_first_7_days, CASE WHEN total_sales = 0 THEN 0 ELSE (total_returns/total_sales)*100 END AS average_return_since_first_order, CASE WHEN max_first_30_last_30_natural_ros > 10 THEN max_first_30_last_30_natural_ros WHEN max_first_30_last_30_natural_ros < 10 AND max_first_30_last_30_natural_ros_first_7_days > 20 THEN max_first_30_last_30_natural_ros_first_7_days WHEN total_sales > 800 THEN GREATEST(natural_ros, sales_last_7_days / 7) ELSE natural_ros END AS final_ros FROM total_sales ) SELECT * FROM enhanced_total_sales ORDER BY total_sales DESC",
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
            