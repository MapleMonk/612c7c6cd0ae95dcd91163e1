{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE redtape_db.maplemonk.redtape_db_Mazhar_FactItems AS SELECT REPLACE(sku,\'`\',\'\') AS sku, REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(sku,\'`\',\'\'),\'^[^_]*_\',\'\'),\'-[A-Za-z0-9]+$\',\'\') ARTICLE, order_date::DATE AS order_date, category_mapped, division, gender, city, SUM(quantity) AS sale_quantity, SUM(gross_sales_before_tax) AS sales_value, SUM(selling_price) AS selling_price, 0 AS return_quantity, 0 AS return_sales FROM redtape_db.maplemonk.redtape_db_sales_consolidated WHERE UPPER(order_status) IN ( \'ASSIGNED\',\'CONFIRMED\',\'MANIFEST SCANNED\',\'OPEN\', \'PRINTED\',\'READY TO DISPATCH\',\'RETURNED\',\'SHIPPED\') GROUP BY 1,2,3,4,5,6,7 UNION ALL SELECT REPLACE(sku,\'`\',\'\') AS sku, REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(sku,\'`\',\'\'),\'^[^_]*_\',\'\'),\'-[A-Za-z0-9]+$\',\'\') ARTICLE, return_date::DATE AS order_date, category_mapped, division, gender, Null as city, 0 AS sale_quantity, 0 AS sales_value, 0 AS selling_price, SUM(total_returned_quantity) AS return_quantity, SUM(total_return_amount) AS return_sales FROM redtape_db.maplemonk.redtape_db_returns_consolidated GROUP BY 1,2,3,4,5,6,7;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from REDTAPE_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            