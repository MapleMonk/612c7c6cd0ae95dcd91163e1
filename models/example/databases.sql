{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.daily_inventory as select skucode sku ,case when category like \'%SHIRT%\' or category like \'%JACKET%\' then \'Top\' when category like \'%SHORTS%\' or category like \'%TROUSER%\' then \'Bottom\' else \'Others\' end product_type ,facility ,inventory ,b.name ,category ,sub_Category ,size ,style_no ,image_link ,selling_price ,mrp from ( select * from ( select *, row_number() over (partition by skucode, facility order by _airbyte_normalized_At desc) rw from MAPLEMONK.Unicommerce_Unicommerce_get_inventory_snapshot ) where rw = 1 ) a left join ( select distinct SPLIT(product_code, \'_\')[OFFSET(0)] sku , name , image_url image_link , color style_no , upper(category_name) category , upper(description) sub_category , upper(size) size , mrp , base_price selling_price from maplemonk.banana_club_unicommerce_get_product_master where color <> \'\' qualify row_number() over(partition by lower(trim(SPLIT(product_code, \'_\')[OFFSET(0)])) order by 1 ) = 1 ) b on trim(lower(a.skucode)) = trim(lower(b.sku));",
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
            