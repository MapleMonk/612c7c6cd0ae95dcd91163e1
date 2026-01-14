{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_Abandon_Checkouts_Report AS with Abandon_Cart AS ( SELECT email, name, cast(created_at AS date) AS Created_at, updated_at, JSON_EXTRACT_SCALAR(line_item, \'$.title\') AS Lineitem_Name, JSON_EXTRACT_SCALAR(line_item, \'$.sku\') AS SKU, CAST(JSON_EXTRACT_SCALAR(line_item, \'$.price\') AS FLOAT64) AS LineItem_Price, CAST(JSON_EXTRACT_SCALAR(line_item, \'$.quantity\') AS INT64) AS LineItem_Quantity, JSON_EXTRACT_SCALAR(shipping_address, \'$.name\') AS Shipping_Name, JSON_EXTRACT_SCALAR(shipping_address, \'$.phone\') AS Shipping_Phone, JSON_EXTRACT_SCALAR(shipping_address, \'$.zip\') AS Shipping_Zip, JSON_EXTRACT_SCALAR(shipping_address, \'$.country\') AS Shipping_Country, JSON_EXTRACT_SCALAR(shipping_address, \'$.province_code\') AS Shipping_Province, JSON_EXTRACT_SCALAR(shipping_address, \'$.city\') AS Shipping_City, landing_site, referring_site FROM `MapleMonk.Shopify_Zouk_abandoned_checkouts` CROSS JOIN UNNEST(line_items) AS line_item QUALIFY ROW_NUMBER() OVER ( PARTITION BY cast(created_at AS date), email, JSON_EXTRACT_SCALAR(line_item, \'$.sku\') ORDER BY updated_at DESC ) = 1 ), Orders AS ( SELECT CAST(order_timestamp AS DATE) AS Order_Date, order_name, email, STRING_AGG(PRODUCT_NAME_Final, \', \') AS Converted_Products, Marketplace FROM `MapleMonk.zouk_shopify_fact_items` WHERE lower(order_status) not like \'%cancel%\' GROUP BY 1, 2, 3, 5 ), Joined_Data as ( SELECT fi.* ,o.Order_Date ,o.Order_name ,o.Converted_Products ,coalesce(p.commonsku, p1.commonsku, fi.SKU) AS SKU_CODE ,Upper(coalesce(p.name, p1.name, fi.lineitem_name)) as PRODUCT_NAME_Final ,coalesce(Upper(p.CATEGORY), p1.CATEGORY) AS Product_Category ,Upper(coalesce(p.sub_category, p1.sub_category)) as Product_Sub_Category FROM Abandon_Cart fi LEFT JOIN ( SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY LOWER(REPLACE(marketplace_sku, \' \', \'\')) ORDER BY LENGTH(IFNULL(Category_Code,\'\')) DESC) rw FROM `zouk-wh.maplemonk.final_sku_master` ) WHERE rw = 1 ) p ON LOWER(REPLACE(fi.sku, \' \', \'\')) = LOWER(REPLACE(p.Marketplace_sku, \' \', \'\')) LEFT JOIN ( SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY LOWER(REPLACE(Channel_Product_Id, \' \', \'\')) ORDER BY LENGTH(IFNULL(Category_Code,\'\')) DESC) rw FROM `zouk-wh.maplemonk.final_sku_master` ) WHERE rw = 1 ) p1 ON LOWER(REPLACE(fi.sku, \' \', \'\')) = LOWER(REPLACE(p1.channel_product_id, \' \', \'\')) LEFT JOIN Orders o ON trim(lower(fi.email)) = trim(lower(o.email)) AND o.Order_Date >= fi.created_at ) SELECT * FROM Joined_Data QUALIFY ROW_NUMBER() OVER ( PARTITION BY email, created_at, SKU ORDER BY Order_Date ASC ) = 1",
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
            