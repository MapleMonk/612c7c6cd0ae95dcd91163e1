{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE sirona-wh-485107.MAPLEMONK.sirona_NYKAA_SALES AS SELECT * FROM `quickmonk.MapleMonk.nykaa_sales_report` WHERE loweR(`Company Name`) LIKE \'%sirona%\' ; CREATE OR REPLACE TABLE sirona-wh-485107.maplemonk.sirona_nykaa_sales_fact_items_intermediate AS SELECT `SKU Code` AS sku_code, `SKU Name` AS product_name, `brand` AS brand, `seller_code`, `Company Name` AS company_name, `Seller Type` AS seller_type, `Platform` AS platform, CAST(`date` AS TIMESTAMP) AS order_date, CAST(`MRP` AS FLOAT64) AS MRP, CAST(`Display Price` AS FLOAT64) AS display_price, CAST(`Selling Price` AS FLOAT64) AS selling_price, CAST(`Total Qty` AS INT64) AS quantity, CAST(`Total Orders` AS INT64) AS total_orders, CAST(`Total Customers` AS INT64) AS total_customers, `Category L1` AS category_l1, `Category L2` AS category_l2, `Category L3` AS category_l3 FROM sirona-wh-485107.MAPLEMONK.nykaa_sales_report qualify row_number() over (partition by date, `SKU Code`, platform order by 1) = 1 ; CREATE OR REPLACE TABLE sirona-wh-485107.maplemonk.sirona_nykaa_sales_fact_items AS SELECT n.*, CONCAT(sku_code, \'-\', order_date, \'-\', platform, \'-\', product_name, \'-\', quantity, \'-\', selling_price) AS order_id, UPPER(TRIM(n.product_name)) AS product_name_final, COALESCE(UPPER(CAST(n.category_l1 AS STRING))) AS product_category, COALESCE(UPPER(n.category_l2)) AS sub_category, COALESCE(UPPER(n.category_l3)) AS sub_sub_category FROM sirona-wh-485107.maplemonk.sirona_nykaa_sales_fact_items_intermediate n LEFT JOIN ( select MRP, GST_, Name, Type, Portal, Portal_Code, Sub_Category, Sirona_SKU_Code from maplemonk.sirona_db_google_sheet_MP_Master qualify row_number() over(partition by portal, Portal_Code order by 1)=1 ) mp on UPPER(n.sku_code) = UPPER(mp.portal_code) AND UPPER(mp.Portal) = \'NYKAA\' ;",
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
            