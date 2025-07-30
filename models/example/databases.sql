{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE trase-wh.maplemonk.TRASE_ASP_BR_TRAFFIC_FACT_ITEMS AS select cast(DATAENDTIME as date) Date ,\'AMAZON\' Marketplace ,parentasin ASIN ,sm.new_sku as SKU ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(trafficbyasin,\'$.browserPageViews\') as float64),0)) Browser_Page_Views ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(trafficbyasin,\'$.browserSessions\') as float64),0)) Browser_Sessions ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(trafficbyasin,\'$.buyBoxPercentage\') as float64),0)) BuyBox_Percentage ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(trafficbyasin,\'$.mobileAppPageViews\') as float64),0)) MobileApp_Page_Views ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(trafficbyasin,\'$.mobileAppSessions\') as float64),0)) MobileApp_Sessions ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(trafficbyasin,\'$.pageViews\') as float64),0)) Page_Views ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(trafficbyasin,\'$.sessions\') as float64),0)) Sessions ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(salesbyasin,\'$.unitsOrdered\') as float64),0)) SC_UnitsOrdered ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(salesbyasin,\'$.totalOrderItems\') as float64),0)) SC_ItemsOrdered ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(JSON_EXTRACT(salesbyasin,\'$.orderedProductSales\'),\'$.amount\') as float64),0)) SC_Sales from `trase-wh.maplemonk.Amazon_Seller_partner_Business_Reports_Trase_Amazon_BR_GET_SALES_AND_TRAFFIC_REPORT_ASIN` abr left join (select new_sku, marketplace, marketplace_id from trase-wh.maplemonk.final_sku_master where lower(marketplace) like \'%amazon%\' qualify row_number() over(partition by trim(marketplace_id) order by 1) = 1 ) sm on sm.marketplace_id = abr.parentasin group by cast(DATAENDTIME as date), Marketplace, parentasin, sm.new_sku ;",
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
            