{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table Maplemonk.Healthy_Master_PNL_Fact_Items as select awb, sc.marketplace, coalesce(sc.channel,mc.marketing_channel) Channel, order_id, reference_code, sku, sku_code, commonsku, product_id, coalesce(sc.order_date,mc.date) order_date, order_time, mrp_sales, QUANTITY, mrp_sales * Quantity as MRP, sm.cogs, sm.cogs * sc.quantity as SALE_COGS, CASE WHEN IFNULL(returned_sales,0) > 0 THEN sm.cogs * sc.quantity ELSE 0 END AS return_cogs, CASE WHEN IFNULL(cancelled_quantity,0) > 0 THEN sm.cogs * sc.cancelled_quantity ELSE 0 END AS cancelled_cogs, GROSS_SALES_BEFORE_TAX, DISCOUNT, tax, CASE WHEN IFNULL(returned_sales,0) > 0 THEN tax ELSE 0 END AS return_tax, SHIPPING_PRICE, SELLING_PRICE, CASE WHEN IFNULL(cancelled_quantity,0) >0 THEN SELLING_PRICE ELSE 0 END AS Cancelled_sales, coalesce(sm.product_title, sc.PRODUCT_NAME_FINAL) PRODUCT_NAME_FINAL, PRODUCT_CATEGORY, coalesce(sm.PRODUCT_TYPE, sc.product_type) PRODUCT_TYPE, FINAL_SHIPPING_STATUS, city, state, PAYMENT_GATEWAY, Payment_Mode, COURIER, returned_quantity, returned_sales, cancelled_quantity, shipment_cost, coalesce(spend,0) spend, coalesce(ad_impressions,0) ad_impressions, coalesce(ad_clicks,0) ad_clicks, coalesce(ad_conversions,0) ad_conversions, coalesce(ad_sales,0) ad_sales from maplemonk.healthymaster_wh_471810_sales_consolidated sc full outer join( select date ,case when lower(channel) like any (\'%facebook%\', \'%google%\') then \'WEBSITE D2C\' else upper(channel) end as Marketplace ,upper(channel) marketing_Channel ,sum(ifnull(spend,0)) as spend ,sum(ifnull(impressions,0)) as ad_impressions ,sum(ifnull(clicks,0)) as ad_clicks ,sum(ifnull(conversions,0)) as ad_conversions ,sum(ifnull(conversion_value,0)) as ad_sales from healthymaster-wh-471810.maplemonk.healthymaster_wh_471810_MARKETING_CONSOLIDATED group by 1,2,3 ) mc on date(sc.order_Date) = date(MC.date) and lower(SC.channel)=lower(MC.marketing_channel) left join( select platform, Shopify_SKU, CAST(NULLIF(REPLACE(COGS, \',\', \'\'), \'#N/A\')AS float64) cogs, product_type, PRODUCT_TITLE from `maplemonk.gs_Updated_SKU_MASTER` where lower(platform) = \'shopify\' qualify row_number() over (partition by shopify_sku order by cast(cogs as float64) desc)=1) sm on UPPER(sc.SKU) = UPPER(sm.shopify_sku) LEFT JOIN( SELECT distinct JSON_EXTRACT_SCALAR(A._airbyte_data,\'$.trackId\') AS trackId, SAFE_CAST(JSON_EXTRACT_SCALAR(A._airbyte_data,\'$.shipment_cost\') AS FLOAT64) AS shipment_cost, JSON_EXTRACT_SCALAR(A._airbyte_data,\'$.payment_type\') AS payment_type, FROM `maplemonk._airbyte_raw_Vamaship_HM_surface_tracking` A where JSON_EXTRACT_SCALAR(A._airbyte_data,\'$.trackId\') is not null) shc on sc.awb = shc.trackId WHERE UPPER(sc.marketplace) LIKE (\'%WEBSITE%\');",
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
            