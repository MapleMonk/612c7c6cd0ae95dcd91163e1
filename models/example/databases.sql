{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE FUAARK_DB.MAPLEMONK.FINAL_SKU_MASTER AS with lisitng as (select * from (select upper(\"SKU Code\") COMMONSKU ,upper(\"Seller SKU on Channel\") MARKETPLACE_SKU ,upper(\"Channel Code\") MARKETPLACE ,row_number() over (partition by Upper(\"Channel Code\"), upper(\"Seller SKU on Channel\") order by 1 desc) rw from fuaark_db.maplemonk.fuaark_sku_listing ) where rw = 1 ), COMMONSKU_MASTER as (select * from (select commonsku , upper(\"PRODUCT TITLE\") name , upper(category) category , upper(collection) sub_category , upper(colour) colour , upper(gender) GENDER , row_number() over (partition by commonsku order by 1) rw from FUAARK_DB.MAPLEMONK.sku_master ) where rw=1 ) select l.COMMONSKU skucode ,l.MARKETPLACE_SKU ,l.MARKETPLACE ,cm.name ,cm.category ,cm.sub_category ,cm.colour ,cm.gender from lisitng l left join COMMONSKU_MASTER CM on upper(l.commonsku) = upper(cm.commonsku);",
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
                        