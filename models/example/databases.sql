{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.Pri_Sec_table AS WITH primary_table AS ( SELECT ROW_NUMBER() OVER (ORDER BY DATE) AS rn, DATE AS primary_date, RETAILER_NAME, AREA_CLASSIFICATION, CASE WHEN area_classification = \'Bangalore-GT\' THEN \'BLR\' WHEN area_classification = \'Hyderabad-GT\' THEN \'HYD\' ELSE \'NCR\' END AS primary_area, PRODUCT_TYPE, CASE WHEN UPPER(TRIM(product_type)) = \'EVERYDAY\' THEN \'Everyday\' ELSE \'Nutrition\' END AS primary_product, RETAILER_CATEGORY, RETAILER_TYPE, DISTRIBUTOR, REVENUE, EGGS_SOLD, EGGS_REPLACED, EGGS_RETURN, SALE_TYPE FROM primary_and_secondary_sku WHERE LOWER(RETAILER_TYPE) IN (\'distributor\') AND DATE >= \'2025-04-01\' ), Projected_Revenue AS ( SELECT primary_area AS projected_area, primary_product AS projected_product, DATE_TRUNC(\'month\', primary_date) AS projected_month_start, SUM(Revenue) AS MTD, MAX(DAY(primary_date)) AS days_elapsed, DAY(LAST_DAY(DATE_TRUNC(\'month\', primary_date))) AS total_days_in_month, ROUND( SUM(Revenue) * ( DAY(LAST_DAY(DATE_TRUNC(\'month\', primary_date))) / NULLIF(MAX(DAY(primary_date)), 0) ), 2 ) AS Trajectory FROM primary_table WHERE DATE_TRUNC(\'month\', primary_date) = DATE_TRUNC(\'month\', CURRENT_DATE) GROUP BY primary_area, primary_product, DATE_TRUNC(\'month\', primary_date) ) SELECT * FROM primary_table LEFT JOIN Projected_Revenue ON primary_table.primary_area = Projected_Revenue.projected_area AND primary_table.primary_product = Projected_Revenue.projected_product AND DATE_TRUNC(\'month\', primary_table.primary_date) = Projected_Revenue.projected_month_start LEFT JOIN bi__target_gt ON DATE_TRUNC(\'month\', primary_table.primary_date) = DATE_TRUNC(\'month\', bi__target_gt.Formatted_date) AND primary_table.primary_area = bi__target_gt.area AND UPPER(primary_table.primary_product) = UPPER(bi__target_gt.parameter);",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from EGGOZDB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            