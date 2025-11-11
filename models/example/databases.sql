{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `izf-wh.maplemonk.izf_neon_market_fact_items` AS SELECT cast(gross as float64) gross_price, sku, split(sku,\'-\')[offset(0)] style, split(sku,\'-\')[offset(1)] color, variant as size, cast(qty as int64) as quantity, PARSE_DATE(\'%Y-%m-%d\', Sold_On) AS order_date, replace(product,\'IZF - \',\'\') as Product_name, CAST(Neon_Market_Commission_25_ AS FLOAT64) AS neon_market_commission_25pct, CAST(Brand_Payout AS FLOAT64) AS selling_price, p.category as product_Category, p.name as product_name_final, concat(\'<img src=\"\',image_url,\'\"width=\"70\">\') as Image FROM `MapleMonk.IZF_GS_Neon_Market_Sales` n left join (select * from (select sku as skucode, Product_Name as name, category_name as category, product_image_url as image_url, row_number() over (partition by SKU order by 1) rw from izf-wh.maplemonk.easyecom_izf_product_master ) where rw = 1 ) p on lower(n.sku) = lower(p.skucode) ;",
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
            