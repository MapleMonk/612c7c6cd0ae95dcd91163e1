{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.Buildskill_AMAZON_VENDOR_PARTNER_SALES_intermediate AS SELECT \'AMAZON VC CRPL\' AS marketplace, \'AMAZON VC CRPL\' AS CHANNEL, \'AMAZON VC CRPL\' AS SOURCE, cast(null as string) AS ORDER_ID, cast(null as string) AS reference_code, cast(null as string) AS SALEORDERITEMCODE, cast(null as string) AS SALES_ORDER_ITEM_ID, CAST(asin AS STRING) AS Asin, CAST(startDate AS TIMESTAMP) AS startTime, CAST(endDate AS TIMESTAMP) AS endTime, CAST(orderedUnits AS INT64) AS Ordered_Units, CAST(JSON_VALUE(orderedRevenue, \'$.amount\') AS FLOAT64) AS Ordered_Revenue, CAST(shippedUnits AS INT64) AS ShippedUnits, CAST(customerReturns AS INT64) AS CustomerReturns FROM `MapleMonk.GET_VENDOR_SALES_REPORT` ; CREATE OR REPLACE TABLE MAPLEMONK.Buildskill_AMAZON_VENDOR_PARTNER_SALES AS select v.* ,p.name AS product_name_final ,COALESCE(UPPER(cast(p.CATEGORY as string))) AS product_category ,UPPER(cast(p.sub_category as string)) AS product_sub_category ,cast(null as string) AS style ,cast(p.skucode as string) as commonsku from MAPLEMONK.Buildskill_AMAZON_VENDOR_PARTNER_SALES_intermediate v left join (select * from maplemonk.sku_master) p on lower(replace(v.asin,\' \',\'\')) = lower(replace(p.skucode,\' \',\'\')) ;",
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
            