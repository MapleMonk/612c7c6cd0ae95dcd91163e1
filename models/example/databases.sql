{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.BUILDSKILL_DB_bigbasket_PRODUCT_LEVEL AS WITH sku_map AS ( SELECT * FROM `maplemonk.Buildskill_GS_SKU_MASTER` QUALIFY ROW_NUMBER() OVER (PARTITION BY master_sku) = 1 ), bigbasket_sales AS ( SELECT TRIM(source_sku_id) AS product_id, DATE(date_range_end) AS order_date, source_city_name AS city, SUM(IFNULL(SAFE_CAST(Total_sales AS FLOAT64), 0)) AS sales, SUM(IFNULL(SAFE_CAST(Total_quantity AS FLOAT64), 0)) AS total_quantity FROM `maplemonk.Bigbasket_sales_analytics_manufacturer_sales_gamma` GROUP BY 1, 2, 3 ), bigbasket_inventory as ( select trim(sku_id) as product_id, city, date(crawl_date) as date,sum(ifnull(safe_cast(SOH as float64),0)) as inventory from `maplemonk.Bigbasket_Inventory__Qoh__qoh_bbsambdh` group by 1,2,3 ) select coalesce(bs.PRODUCT_ID, bi.product_id) as product_id, coalesce(date(bs.order_date), date(bi.date)) AS DATE, FORMAT_DATE(\'%A\', coalesce(date(bs.order_date), date(bi.date))) AS DAY_OF_WEEK, EXTRACT(YEAR FROM coalesce(date(bs.order_date), date(bi.date))) AS YEAR, EXTRACT(MONTH FROM coalesce(date(bs.order_date), date(bi.date))) AS MONTH, \'BigBasket\' AS CHANNEL, UPPER(TRIM(sm.master_sku)) AS SKU, UPPER(TRIM(sm.PRODUCT_TITLE)) AS product_name, CONCAT(UPPER(TRIM(sm.CATEGORY)), \' \', UPPER(TRIM(sm.SUB_CATEGORY))) AS master_category, coalesce(bs.city, bi.city) as city, SUM(IFNULL(bs.sales, 0)) AS sales, SUM(IFNULL(bs.total_quantity, 0)) AS total_quantity, sum(ifnull(bi.inventory,0)) as total_inventory FROM bigbasket_sales bs full outer join bigbasket_inventory bi on trim(bs.product_id) = trim(bi.product_id) and upper(trim(bs.city)) = upper(trim(bi.city)) and date(bs.order_date) = date(bi.date) LEFT JOIN sku_map sm ON TRIM(LOWER(bs.product_id)) = TRIM(LOWER(sm.bigbasket_id)) group by 1,2,3,4,5,6,7,8,9,10; CREATE OR REPLACE TABLE MAPLEMONK.BUILDSKILL_CATEGORY_DAILY_SUMMARY_bigbasket AS SELECT DATE, DATE_TRUNC(DATE, MONTH) AS month_start, master_category, product_id, product_name, city, SUM(IFNULL(sales,0)) AS daily_sales, SUM(IFNULL(total_quantity,0)) AS total_quantity, sum(ifnull(total_inventory,0)) as total_inventory FROM MAPLEMONK.BUILDSKILL_DB_bigbasket_PRODUCT_LEVEL a GROUP BY 1,2,3,4,5,6;",
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
            