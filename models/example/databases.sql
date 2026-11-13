{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE thd-pharma-wh.maplemonk.thd_pharma_nykaa_sales_fact_items_intermediate AS SELECT `SKU Code` AS sku_code, `SKU Name` AS product_name, `brand` AS brand, `seller_code`, `Company Name` AS company_name, `Seller Type` AS seller_type, `Platform` AS platform, CAST(`date` AS TIMESTAMP) AS order_date, CAST(`MRP` AS FLOAT64) AS MRP, CAST(`Display Price` AS FLOAT64) AS display_price, CAST(`Selling Price` AS FLOAT64) AS selling_price, CAST(`Total Qty` AS INT64) AS quantity, CAST(`Total Orders` AS INT64) AS total_orders, CAST(`Total Customers` AS INT64) AS total_customers, `Category L1` AS category_l1, `Category L2` AS category_l2, `Category L3` AS category_l3 FROM MAPLEMONK.nykaa_sales_report qualify row_number() over (partition by date, `SKU Code`, platform order by 1) = 1 ; CREATE OR REPLACE TABLE thd-pharma-wh.maplemonk.thd_pharma_nykaa_sales_fact_items AS SELECT CONCAT(sku_code, \'-\', order_date, \'-\', platform, \'-\', n.product_name, \'-\', quantity, \'-\', selling_price) AS order_id, p.master_sku, coalesce(n.mrp,p.mrp) as mrp, UPPER(coalesce(TRIM(n.product_name),p.product_name)) AS product_name_final, COALESCE(UPPER(CAST(n.category_l1 AS STRING))) AS product_category, COALESCE(UPPER(n.category_l2),p.sub_category) AS sub_category, COALESCE(UPPER(n.category_l3)) AS sub_sub_category, n.* except (mrp) FROM thd-pharma-wh.maplemonk.thd_pharma_nykaa_sales_fact_items_intermediate n LEFT JOIN (SELECT upper(trim(nykaa)) nykaa_sku, upper(trim(sku)) as master_sku, upper(trim(productname)) as product_name, cast(mrp as float64) as mrp, upper(trim(sub_category)) as sub_category from maplemonk.inventory_product_MASTER where upper(trim(nykaa)) is not null qualify row_number() over (partition by upper(nykaa) order by sku) = 1 ) p on upper(p.nykaa_sku) = upper(REPLACE(n.sku_code, \'\"\', \'\')) ;",
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
            