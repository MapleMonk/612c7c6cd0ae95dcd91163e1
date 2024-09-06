{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE FUAARK_DB.MAPLEMONK.FINAL_SKU_MASTER AS with lisitng as (select * from (select upper(\"SKU Code\") COMMONSKU ,upper(\"Seller SKU on Channel\") MARKETPLACE_SKU ,upper(\"Channel Code\") MARKETPLACE ,row_number() over (partition by Upper(\"Channel Code\"), upper(\"Seller SKU on Channel\") order by 1 desc) rw from fuaark_db.maplemonk.fuaark_unicommerce_get_product_listing ) where rw = 1 ), COMMONSKU_MASTER as ( WITH COLOR_LIST AS ( SELECT DISTINCT UPPER(REPLACE(color, \'&\', \'with\')) AS COLORS , length(color) FROM fuaark_db.maplemonk.unicommerce_unicommerce_get_product_master where COLOR is not null and trim(COLOR) != \'\' order by 2 desc ), COLORS_PATTERN AS ( SELECT ARRAY_TO_STRING(ARRAY_AGG(COLORS), \'|\') AS COLORS_REGEX FROM COLOR_LIST ) Select * from ( SELECT upper(Name) as NAME ,upper(COLOR) as colour ,upper(BRAND) as BRAND ,upper(SIZE) as SIZE ,upper(\"Category Name\") CATEGORY ,upper(\"Product Code\") commonsku ,null as GENDER ,SPLIT(TRIM(REGEXP_REPLACE(UPPER(NAME),COLORS_REGEX, \'\')),\' - \')[0]::String AS sub_category ,row_number() over (partition by commonsku order by 1) rw FROM fuaark_db.maplemonk.unicommerce_unicommerce_get_product_master, COLORS_PATTERN )where rw = 1 ) select l.COMMONSKU skucode ,l.MARKETPLACE_SKU ,l.MARKETPLACE ,cm.name ,cm.category ,cm.sub_category ,cm.colour ,cm.gender from lisitng l left join COMMONSKU_MASTER CM on upper(l.commonsku) = upper(cm.commonsku);",
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
            