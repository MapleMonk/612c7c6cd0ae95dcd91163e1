{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE qurez-wh.maplemonk.qurez_nykaa_sales_fact_items_intermediate AS SELECT `SKU Code` AS sku_code, `SKU Name` AS sku_name, `brand` AS brand, `seller_code`, `Company Name` AS company_name, `Seller Type` AS seller_type, `Platform` AS platform, CAST(`date` AS TIMESTAMP) AS order_date, CAST(`MRP` AS FLOAT64) AS MRP, CAST(`Display Price` AS FLOAT64) AS display_price, CAST(`Selling Price` AS FLOAT64) AS selling_price, CAST(`Total Qty` AS INT64) AS quantity, CAST(`Total Orders` AS INT64) AS total_orders, CAST(`Total Customers` AS INT64) AS total_customers, `Category L1` AS category_l1, `Category L2` AS category_l2, `Category L3` AS category_l3 FROM qurez-wh.MAPLEMONK.nykaa_sales_report qualify row_number() over (partition by date, `SKU Code`, platform order by 1) = 1 ; CREATE OR REPLACE TABLE qurez-wh.maplemonk.qurez_nykaa_sales_fact_items AS SELECT n.*, CONCAT(n.sku_code, \'-\', order_date, \'-\', platform, \'-\', sku_name, \'-\', quantity, \'-\', n.selling_price) AS order_id, UPPER(coalesce(TRIM(n.sku_name))) AS product_name_final, UPPER(CAST(n.category_l1 AS STRING)) AS product_category, UPPER(n.category_l2) AS sub_category, UPPER(n.category_l3) AS sub_sub_category FROM qurez-wh.maplemonk.qurez_nykaa_sales_fact_items_intermediate n LEFT JOIN ( SELECT * FROM `qurez-wh.maplemonk.Qurez_SKU_Master` WHERE LOWER(final_marketplace) LIKE \'%nykaa%\' QUALIFY ROW_NUMBER() OVER ( PARTITION BY UPPER(SKU_CODE) ORDER BY SKU_CODE ) = 1 ) sk ON UPPER(sk.sku_code) = UPPER(n.sku_code) ;",
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
            