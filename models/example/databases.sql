{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Geek_AMAZON_VENDOR_PARTNER AS SELECT CAST(asin AS STRING) AS Asin, DATETIME(startTime, \'Asia/Kolkata\') AS StartTime, datetime(endtime,\"Asia/Kolkata\") AS EndTime, CAST(orderedUnits AS INT64) AS OrderedUnits, CAST(orderedRevenue AS float64) AS OrderedRevenue, p.name as product_name_final, p.category as product_category, p.sub_category as product_sub_category FROM `MapleMonk.Amazon_VP_Geek_GET_VENDOR_REAL_TIME_SALES_REPORT` fi left join (select * from (select parent_asin as skucode, sku as commonsku, parent_category as category, child_category as sub_category, product_int_name as name, row_number()over (partition by parent_asin order by length(ifnull(parent_asin,\'\')) desc) rw from geek-maplemonk.maplemonk.geek_technology_sku_master ) where rw = 1 ) p on lower(replace(fi.Asin,\' \',\'\')) = lower(p.skucode) ;",
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
            