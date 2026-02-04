{{ config(
            materialized='table',
                post_hook={
                    "sql": "drop table public.tuco_kids_bigbasket_fact_items; create table public.tuco_kids_bigbasket_fact_items as SELECT CAST(total_mrp AS FLOAT) AS mrp, TO_DATE(SPLIT_PART(date_range, \' - \', 1), \'YYYYMMDD\') AS order_date, date_range, CAST(total_sales AS FLOAT) AS selling_price, CAST(REPLACE(total_quantity, \'.0\', \'\') AS INTEGER) AS quantity, source_city_name AS city, REPLACE(REPLACE(UPPER(sku_description), \'HEALTHY MASTER \', \'\'), \'-\', \' \') AS product_name, UPPER(REPLACE(REPLACE(leaf_slug, \'snacks\', \'\'), \'-\', \' \')) AS product_category FROM public.bigbasket_analytics_manufacturer_sales;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            