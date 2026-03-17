{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE skinbae-wh.maplemonk.skinbae_nykaa_sales_fact_items_intermediate AS SELECT `SKU Code` AS sku_code, `SKU Name` AS sku_name, `brand` AS brand, `seller_code`, `Company Name` AS company_name, `Seller Type` AS seller_type, `Platform` AS platform, CAST(`date` AS TIMESTAMP) AS order_date, CAST(`MRP` AS FLOAT64) AS MRP, CAST(`Display Price` AS FLOAT64) AS display_price, CAST(`Selling Price` AS FLOAT64) AS selling_price, CAST(`Total Qty` AS INT64) AS quantity, CAST(`Total Orders` AS INT64) AS total_orders, CAST(`Total Customers` AS INT64) AS total_customers, `Category L1` AS category_l1, `Category L2` AS category_l2, `Category L3` AS category_l3 FROM `quickmonk.MapleMonk.nykaa_sales_report` WHERE `Company Name` LIKE \'%Sqintalk Cosmeceuticals Private Limited%\' qualify row_number() over (partition by date, `SKU Code`,platform order by date) = 1 union all SELECT `SKU Code` AS sku_code, `SKU Name` AS sku_name, `brand` AS brand, `seller_code`, `Company Name` AS company_name, `Seller Type` AS seller_type, `Platform` AS platform, CAST(`date` AS TIMESTAMP) AS order_date, CAST(`MRP` AS FLOAT64) AS MRP, CAST(`Display Price` AS FLOAT64) AS display_price, CAST(`Selling Price` AS FLOAT64) AS selling_price, CAST(`Total Qty` AS INT64) AS quantity, CAST(`Total Orders` AS INT64) AS total_orders, CAST(`Total Customers` AS INT64) AS total_customers, `Category L1` AS category_l1, `Category L2` AS category_l2, `Category L3` AS category_l3 FROM `quickmonk.MapleMonk.nykaa_sales_report` WHERE `Company Name` LIKE \'%Squintalk Cosmeceuticals Pvt Ltd%\' qualify row_number() over (partition by date, `SKU Code`,platform order by date) = 1 ; CREATE OR REPLACE TABLE skinbae-wh.maplemonk.skinbae_nykaa_sales_fact_items AS SELECT n.*, CONCAT(sku_code, \'-\', order_date, \'-\', platform, \'-\', sku_name, \'-\', quantity, \'-\', selling_price) AS order_id, UPPER(coalesce(p.product_name, TRIM(n.sku_name))) AS product_name_final, COALESCE(UPPER(cast(p.CATEGORY as string)), UPPER(CAST(n.category_l1 AS STRING))) AS product_category, COALESCE(UPPER(cast(p.sub_CATEGORY as string)), UPPER(n.category_l2)) AS sub_category, COALESCE(UPPER(n.category_l3)) AS sub_sub_category FROM maplemonk.skinbae_nykaa_sales_fact_items_intermediate n LEFT JOIN (SELECT nykaa_sku, master_sku, product_name, category, sub_category from maplemonk.final_SKU_MASTER qualify row_number() over (partition by nykaa_sku order by master_sku) = 1 ) p on upper(p.nykaa_sku) = upper(REPLACE(n.sku_code, \'\"\', \'\')) ;",
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
            