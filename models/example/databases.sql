{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table ORPAT_DB.MAPLEMONK.ORPAT_DB_recommendation_system as WITH SortedData AS ( SELECT order_date, order_id, MARKETPLACE, ARRAY_AGG(sku) WITHIN GROUP (ORDER BY sku) AS sorted_sku, ARRAY_AGG(quantity) WITHIN GROUP (ORDER BY sku) AS sorted_quantity FROM (SELECT order_date, order_id, MARKETPLACE, sku, SUM(quantity) AS quantity FROM ORPAT_DB.MAPLEMONK.ORPAT_DB_sales_consolidated GROUP BY 1, 2, 3,4) GROUP BY order_date, order_id ,MARKETPLACE ), combinations AS ( SELECT order_date, order_id, MARKETPLACE, ARRAY_TO_STRING(sorted_sku, \' , \') AS concatenated_sku, ARRAY_TO_STRING(sorted_quantity, \' , \') AS concatenated_quantity, replace(sku1,\'\"\',\'\') as sku_1, CASE WHEN sku1 != sku2 THEN replace(sku2,\'\"\',\'\') ELSE NULL END AS sku_2, sum(sorted_quantity[array_position(sku_1::variant, sorted_sku)]::int) as sku1_quantity, sum(sorted_quantity[array_position(sku_2::variant, sorted_sku)]::int) as sku2_quantity FROM ( SELECT o.order_date, o.order_id, o.MARKETPLACE, o.sorted_sku, o.sorted_quantity, s.value AS sku1, s2.value AS sku2, ARRAY_SIZE(sorted_sku) AS length FROM SortedData o, LATERAL FLATTEN(INPUT => o.sorted_sku) s, LATERAL FLATTEN(INPUT => o.sorted_sku) s2 WHERE s.value != s2.value or (length = 1) ) group by 1,2,3,4,5,6,7 ) SELECT c.*, div0(sku1_quantity,count(1) over(partition by order_id,sku_1)) as normalized_sku1_quantity, div0(sku2_quantity,count(1) over(partition by order_id,sku_2)) as normalized_sku2_quantity, s.PRODUCT_SUB_CATEGORY as sku1_sub_category, s1.PRODUCT_SUB_CATEGORY as sku2_sub_category FROM combinations c left join (select distinct sku,product_sub_category from ORPAT_DB.MAPLEMONK.ORPAT_DB_sales_consolidated) s on c.sku_1 = s.sku left join (select distinct sku,product_sub_category from ORPAT_DB.MAPLEMONK.ORPAT_DB_sales_consolidated) s1 on c.sku_2 = s1.sku",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from ORPAT_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            