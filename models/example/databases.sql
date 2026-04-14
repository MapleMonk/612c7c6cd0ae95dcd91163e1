{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.babygo_AMAZON_VENDOR_PARTNER_SALES AS SELECT \'AMAZON VC\' AS marketplace, \'AMAZON VC\' AS CHANNEL, \'AMAZON VC\' AS SOURCE, concat(a.Asin,CAST(startDate AS DATE),orderedUnits,CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64),CAST(endDate AS DATE)) AS ORDER_ID, concat(a.Asin,CAST(startDate AS DATE),orderedUnits,CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64),CAST(endDate AS DATE)) AS reference_code, concat(a.Asin,CAST(startDate AS DATE),orderedUnits,CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64),CAST(endDate AS DATE)) AS SALEORDERITEMCODE, concat(a.Asin,CAST(startDate AS DATE),orderedUnits,CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64),CAST(endDate AS DATE)) AS SALES_ORDER_ITEM_ID, CAST(a.asin AS STRING) AS Asin, CAST(startDate AS TIMESTAMP) AS startTime, CAST(endDate AS TIMESTAMP) AS endTime, CAST(orderedUnits AS INT64) AS Ordered_Units, CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64) AS Ordered_Revenue, CAST(shippedUnits AS INT64) AS ShippedUnits, CAST(customerReturns AS INT64) AS CustomerReturns, safe_divide(CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64) ,CAST(orderedUnits AS INT64)) * CAST(customerReturns AS INT64) returned_revenue, p.name as product_name_final, p.category as product_category, p.sub_Category, p.style_id, p.lot, b.sku sku FROM MapleMonk.BabyGo_Amazon_Vendor_Partner_GET_VENDOR_SALES_REPORT a left join ( select identifier asin, sku from maplemonk.green_orbit_get_marketplace_listing where lower(name) like \'%vendor%\' qualify row_number() over (partition by identifier order by sku) = 1 ) b on a.asin = b.asin left join (select * from (select sku skucode, cast(null as string) commonsku, cast(null as string) name, cast(internal_category as string) category, cast(null as string) as sub_category, style_id, lot, row_number() over (partition by replace(sku,\' \',\'\') order by 1) rw from babygo-wh.Maplemonk.gs_sku_master ) where rw = 1 ) p on lower(replace(b.sku,\' \',\'\')) = lower(replace(p.skucode,\' \',\'\')) ;",
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
            