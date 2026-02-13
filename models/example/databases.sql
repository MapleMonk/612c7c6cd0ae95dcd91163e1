{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table izf-wh.maplemonk.izf_knot_fact_items as select k.SKU, cast(coalesce(k.size,split(replace(UPPER(trim(k.parts[SAFE_OFFSET(ARRAY_LENGTH(k.parts) - 1)])),\'_\',\'\'),\'/Z\')[SAFE_OFFSET(0)]) as string) as sku_size, replace(replace(k.parts[safe_offset(0)],\'`\',\'\'),\'\\'\',\'\') sku_style, cast(FORMAT_DATE(\'%Y-%m-%d\',PARSE_DATE(\'%d-%b-%y\', k.Sold_on))as date) Order_Date, cast(k.quantity as int64) as Quantity, k.Hex_Color, k.Product_URL, k.Color_Family as Colour, k.Product_Name, cast(k.selling_Price as float64) as Selling_Price, p.category as product_category, concat(\'<img src=\"\',image_url,\'\"width=\"70\">\') as Image from (select *,ARRAY( SELECT part FROM UNNEST(SPLIT(replace(sku,\'_\',\'-\'), \'-\')) AS part WHERE part != \'\' ) as parts from maplemonk.izf_knot_sales ) k left join (select * from (select sku as skucode ,Product_Name as name ,category_name as category ,product_image_url as image_url ,row_number() over (partition by SKU order by 1) rw from izf-wh.maplemonk.easyecom_izf_product_master where product_image_url is not null ) where rw = 1 )p on lower(k.sku) = lower(p.skucode) ;",
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
            