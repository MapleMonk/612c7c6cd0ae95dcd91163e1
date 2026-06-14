{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.daily_inventory as select Item_SkuCode sku ,FORMAT_TIMESTAMP(\'%Y-%m-%d\', a._airbyte_normalized_at) AS Date ,case when category like \'%SHIRT%\' or category like \'%JACKET%\' then \'Top\' when category like \'%SHORTS%\' or category like \'%TROUSER%\' then \'Bottom\' else \'Others\' end product_type ,facility ,inventory ,b.name ,category ,sub_Category ,coalesce(b.size,a.size) size ,case when a.style = \'\' or a.style is null then b.style_no else a.style end as style_no ,image_link , coalesce(P.product_link,P.PDP_URL) AS PDP_URL ,selling_price ,coalesce(b.mrp, a.mrp) mrp from ( select * from ( select *, row_number() over (partition by Item_SkuCode, facility order by _airbyte_normalized_At desc) rw from `MAPLEMONK.unicommerce_get_inventory_snapshot_export_full_refresh` ) where rw = 1 ) a left join ( select distinct SPLIT(product_code, \'_\')[OFFSET(0)] sku , name , image_url image_link , color style_no , upper(category_name) category , upper(description) sub_category , upper(size) size , mrp , base_price selling_price from maplemonk.banana_club_unicommerce_get_product_master where color <> \'\' qualify row_number() over(partition by lower(trim(SPLIT(product_code, \'_\')[OFFSET(0)])) order by 1 ) = 1 ) b on trim(lower(a.Item_SkuCode)) = trim(lower(b.sku)) left join( select Distinct SPLIT(product_code, \'_\')[OFFSET(0)] Product_Code ,Style from `MAPLEMONK.BananaClub_DB_get_product_master` where color <> \'\' qualify row_number() over(partition by lower(trim(SPLIT(product_code, \'_\')[OFFSET(0)])) order by 1 ) = 1 ) pm on trim(lower(a.Item_SkuCode)) = trim(lower(pm.Product_Code)) Left Join (select trim(upper(SKU)) AS SKU, PDP_URL, Product_Link from maplemonk.bananaclub_SHOPIFY_FACT_ITEMS Qualify row_number() over (partition by trim(upper(SKU)) order by Product_Link desc) = 1 ) P ON trim(lower(a.Item_SkuCode)) = trim(lower(P.SKU)) ;",
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
            