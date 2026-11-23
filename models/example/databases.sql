{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.izf_slikk_fact_items AS SELECT SAFE_CAST(REPLACE(CAST(sp AS STRING), \',\', \'\') AS FLOAT64) AS selling_price, hsn, SAFE_CAST(REPLACE(CAST(mrp AS STRING), \',\', \'\') AS FLOAT64) AS mrp, sku, SAFE_CAST(REPLACE(CAST(tax AS STRING), \',\', \'\') AS FLOAT64) AS tax, COALESCE(p.name, s.name) AS product_name, size, brand, color, SAFE_CAST(REPLACE(CAST(discount AS STRING), \',\', \'\') AS FLOAT64) AS discount, SAFE_CAST(REPLACE(CAST(tax_rate AS STRING), \',\', \'\') AS FLOAT64) AS tax_rate, DATE(PARSE_DATE(\'%d-%m-%Y\', order_date)) AS order_date, invoice_id, CASE WHEN UPPER(payment_mode) LIKE \'%POD%\' THEN \'COD\' ELSE \'PREPAID\' END AS payment_mode, SAFE_CAST(REPLACE(CAST(total_amount AS STRING), \',\', \'\') AS FLOAT64) AS total_amount, delivery_type, SAFE_CAST(REPLACE(CAST(taxable_amount AS STRING), \',\', \'\') AS FLOAT64) AS gross_sales_before_tax, SAFE_CAST(REPLACE(CAST(total_quantity AS STRING), \',\', \'\') AS INT64) AS quantity, REGEXP_EXTRACT(delivery_address, r\'(\d{6})$\') AS pincode, REGEXP_EXTRACT(delivery_address, r\',([^,]+)-\d{6}$\') AS state, REGEXP_EXTRACT(delivery_address, r\',([^,]+),[^,]+-\d{6}$\') AS city, COALESCE( CONCAT(\'<img src=\"\', p.image_url, \'\"width=\"70\">\'), CONCAT(\'<img src=\"\', s.image, \'\"width=\"70\">\') ) AS Image, p.category AS product_category FROM maplemonk.gs_izf_slikk_sales s LEFT JOIN ( SELECT * FROM ( SELECT sku AS skucode, Product_Name AS name, category_name AS category, product_image_url AS image_url, ROW_NUMBER() OVER (PARTITION BY SKU ORDER BY 1) AS rw FROM izf-wh.maplemonk.easyecom_izf_product_master ) WHERE rw = 1 ) p ON LOWER(s.sku) = LOWER(p.skucode);",
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
            