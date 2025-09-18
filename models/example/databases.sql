{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table freakins-wh.maplemonk.freakins_db_recommendation_system as WITH SortedData AS ( SELECT order_date, order_id, MARKETPLACE, ARRAY_AGG(sku IGNORE NULLS ORDER BY sku) AS sorted_sku, ARRAY_AGG(cast(quantity as string) IGNORE NULLS ORDER BY sku) AS sorted_quantity FROM (SELECT order_date, order_id, MARKETPLACE, coalesce(master_sku,sku) sku, SUM(quantity) AS quantity FROM freakins-wh.maplemonk.freakins_db_sales_consolidated GROUP BY 1, 2, 3,4) GROUP BY order_date, order_id ,MARKETPLACE ), combinations AS ( SELECT order_date, order_id, marketplace, ARRAY_TO_STRING(sorted_sku, \', \') AS concatenated_sku, ARRAY_TO_STRING(sorted_quantity, \', \') AS concatenated_quantity, REPLACE(sku1, \'\"\', \'\') AS sku_1, CASE WHEN sku1 != sku2 THEN REPLACE(sku2, \'\"\', \'\') ELSE NULL END AS sku_2, sum(cast(sorted_quantity[safe_offset((select offset from unnest(sorted_sku) m with offset where m = REPLACE(sku1, \'\"\', \'\')))] as int64)) as sku1_quantity, sum(cast(sorted_quantity[safe_offset((select offset from unnest(sorted_sku) m with offset where m =(CASE WHEN sku1 != sku2 THEN REPLACE(sku2, \'\"\', \'\') ELSE NULL END)))] as int64)) as sku2_quantity FROM ( SELECT o.order_date, o.order_id, o.MARKETPLACE, o.sorted_sku, o.sorted_quantity, s AS sku1, s2 AS sku2, ARRAY_LENGTH(sorted_sku) AS length FROM SortedData o, UNNEST(o.sorted_sku) AS s, UNNEST(o.sorted_sku) AS s2 WHERE s <> s2 or ARRAY_LENGTH(o.sorted_sku) = 1 ) GROUP BY 1,2,3,4,5,6,sku1,sku2 ) SELECT c.*, safe_divide(sku1_quantity,count(1) over(partition by order_id,sku_1)) as normalized_sku1_quantity, safe_divide(sku2_quantity,count(1) over(partition by order_id,sku_2)) as normalized_sku2_quantity, s.Style_code as sku1_sub_category, s1.Style_code as sku2_sub_category, s.Product_Category_Main as SKU1_Category, s1.Product_Category_Main as SKU2_Category FROM combinations c left join (select sku,Style_code, Product_Category_Main from freakins-wh.maplemonk.freakins_db_sales_consolidated qualify row_number() over(partition by lower(sku) order by 1) = 1 ) s on c.sku_1 = s.sku left join (select sku,Style_code ,Product_Category_Main from freakins-wh.maplemonk.freakins_db_sales_consolidated qualify row_number() over(partition by lower(sku) order by 1) = 1 ) s1 on c.sku_2 = s1.sku;",
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
            