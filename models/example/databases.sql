{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE FUAARK_DB.MAPLEMONK.FINAL_SKU_MASTER AS with lisitng as (select * from (select trim(upper(\"SKU Code\")) COMMONSKU ,upper(\"Seller SKU on Channel\") MARKETPLACE_SKU ,upper(\"Channel Code\") MARKETPLACE ,row_number() over (partition by Upper(\"Channel Code\"), upper(\"Seller SKU on Channel\") order by 1 desc) rw from fuaark_db.maplemonk.fuaark_unicommerce_get_product_listing ) where rw = 1 ), COMMONSKU_MASTER as ( Select * from ( SELECT upper(Name) as NAME ,upper(COLOR) as colour ,upper(BRAND) as BRAND ,upper(SIZE) as SIZE ,upper(\"Category Name\") CATEGORY ,trim(upper(\"Product Code\")) commonsku ,upper(subcategories) as GENDER ,upper(case when trim(ifnull(collection,\'\')) != \'\' then collection end) AS sub_category ,row_number() over (partition by commonsku order by 1) rw FROM fuaark_db.maplemonk.unicommerce_fuaark_get_product_master )where rw = 1 ) select coalescE(l.COMMONSKU, cm.commonsku) skucode ,l.MARKETPLACE_SKU ,l.MARKETPLACE ,cm.name ,cm.category ,cm.sub_category ,cm.colour ,cm.gender from lisitng l full outer join COMMONSKU_MASTER CM on upper(l.commonsku) = upper(cm.commonsku);",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from FUAARK_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            