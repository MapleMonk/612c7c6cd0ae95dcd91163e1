{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table MAPLEMONK.Bananaclub_Data_Scrapping_Fact_Items as WITH Myntra_Final AS ( SELECT CAST(DATE(TIMESTAMP(scraped_at)) AS DATE) AS scraped_date, mrp AS Myntra_MRP, brand, CAST(price AS FLOAT64) AS Myntra_price, stock, title, gender, rating, sku_id, page_no, COALESCE(mp.category, sm.Category) AS category, discount, COALESCE(mp.grammage, sm.SIZE) AS grammage, position, image_url, product_url, article_type, is_sponsored, listing_type, rating_count, COALESCE(mp.sub_category, sm.Sub_Category) AS sub_category, primary_color, is_best_seller, inventory_count, discount_display_label, sm.sku AS Summary_SKU, COALESCE(product_id, sm.Myntra_Style_ID) AS Myntra_Product_Id FROM `MAPLEMONK.Bananaclub_myntra_products` mp LEFT JOIN `MAPLEMONK.Google_sheet_bc_Summary` sm ON mp.product_id = sm.Myntra_Style_ID AND mp.grammage = sm.SIZE ), Shopify_Data AS ( SELECT DATE(TIMESTAMP(JSON_EXTRACT_SCALAR(v, \'$.created_at\'))) AS created_date, JSON_EXTRACT_SCALAR(v, \'$.sku\') AS Shopify_sku, JSON_EXTRACT_SCALAR(v, \'$.product_id\') AS Shopify_product_id, JSON_EXTRACT_SCALAR(v, \'$.title\') AS Shopify_size, JSON_EXTRACT_SCALAR(v, \'$.price\') AS Shopify_price, JSON_EXTRACT_SCALAR(v, \'$.compare_at_price\') AS Shopify_compare_at_price FROM `MAPLEMONK.Bananacb_products` q LEFT JOIN UNNEST(q.variants) v ) SELECT mf.*, COALESCE(sd.Shopify_sku, mf.Summary_SKU) AS Shopify_sku, sd.Shopify_product_id, COALESCE(sd.Shopify_size,mf.grammage) AS Size, CAST(sd.Shopify_price AS FLOAT64) Shopify_price, COALESCE(sd.Shopify_compare_at_price,mf.Myntra_MRP) AS MRP, sd.created_date AS Shopify_created_date FROM Myntra_Final mf LEFT JOIN Shopify_Data sd ON mf.Summary_SKU = sd.Shopify_sku AND mf.grammage = sd.Shopify_size;",
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
            