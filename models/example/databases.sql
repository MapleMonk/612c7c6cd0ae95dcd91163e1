{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table redtape_db.maplemonk.redtape_db_Mazhar_FactItems as SELECT COALESCE(a.sku,b.sku) SKU, COALESCE(a.article,b.article) ARTICLE, COALESCE(a.order_date,b.return_date) ORDER_DATE, COALESCE(a.category_mapped,b.category_mapped) CATEGORY_MAPPED, COALESCE(a.division,b.division) DIVISION, COALESCE(a.gender,b.gender) GENDER, city, COALESCE(a.sale_quantity, 0) sale_quantity, COALESCE(a.sales_value, 0) sales_value, COALESCE(a.selling_price, 0) selling_price, COALESCE(b.return_quantity, 0) return_quantity, COALESCE(b.return_sales, 0) return_sales FROM ( SELECT ordeR_date::Date order_date, REPLACE(sku,\'`\',\'\') AS sku, REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(sku,\'`\',\'\'),\'^[^_]*_\',\'\'),\'-[A-Za-z0-9]+$\',\'\') ARTICLE, category_mapped, division, gender, city, SUM(quantity) sale_quantity, SUM(gross_sales_before_tax) sales_value, SUM(selling_price) selling_price, FROM redtape_db.maplemonk.redtape_db_sales_consolidated WHERE UPPER(order_status) IN ( \'ASSIGNED\',\'CONFIRMED\',\'MANIFEST SCANNED\',\'OPEN\', \'PRINTED\',\'READY TO DISPATCH\',\'RETURNED\',\'SHIPPED\') GROUP BY 1,2,3,4,5,6,7) a LEFT JOIN ( SELECT REPLACE(sku,\'`\',\'\') sku, return_date::Date return_date, REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(sku,\'`\',\'\'),\'^[^_]*_\',\'\'),\'-[A-Za-z0-9]+$\',\'\') ARTICLE, category_mapped, division, gender, SUM(total_returned_quantity) AS return_quantity, SUM(total_return_amount) return_sales FROM redtape_db.maplemonk.redtape_db_returns_consolidated GROUP BY 1,2,3,4,5,6) b ON a.sku=b.sku AND a.order_date = b.return_date;",
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
            