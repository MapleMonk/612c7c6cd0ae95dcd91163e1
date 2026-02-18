{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.ASP_BR_TRAFFIC_FACT_ITEMS AS with base_data as( select cast(DATAENDTIME as date) Date ,\'AMAZON SELLER CENTRAL INDIA\' Marketplace ,\'INDIA\' Region ,parentasin ASIN ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(trafficbyasin,\'$.browserPageViews\') as float64),0)) Browser_Page_Views ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(trafficbyasin,\'$.browserSessions\') as float64),0)) Browser_Sessions ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(trafficbyasin,\'$.buyBoxPercentage\') as float64),0)) BuyBox_Percentage ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(trafficbyasin,\'$.mobileAppPageViews\') as float64),0)) MobileApp_Page_Views ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(trafficbyasin,\'$.mobileAppSessions\') as float64),0)) MobileApp_Sessions ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(trafficbyasin,\'$.pageViews\') as float64),0)) Page_Views ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(trafficbyasin,\'$.sessions\') as float64),0)) Sessions ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(salesbyasin,\'$.unitsOrdered\') as float64),0)) SC_UnitsOrdered ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(salesbyasin,\'$.totalOrderItems\') as float64),0)) SC_ItemsOrdered ,sum(ifnull(safe_Cast(JSON_EXTRACT_SCALAR(JSON_EXTRACT(salesbyasin,\'$.orderedProductSales\'),\'$.amount\') as float64),0)) SC_Sales from maplemonk.Amazon_Seller_partner_Business_Reports_ASP_BR_KA_GET_SALES_AND_TRAFFIC_REPORT_ASIN group by cast(DATAENDTIME as date), Marketplace, parentasin, Region ) select fi.Date ,Marketplace ,Region ,fi.ASIN ,p.commonsku ,upper(p.name) Product_Name ,upper(p.category) Product_Category ,upper(p.sub_category) Product_Sub_Category ,Browser_Page_Views ,Browser_Sessions ,BuyBox_Percentage ,MobileApp_Page_Views ,MobileApp_Sessions ,(ifnull(fi.page_views,0) + ifnull(AVCV.Glance_views,0)) Page_Views ,Sessions ,SC_UnitsOrdered ,SC_ItemsOrdered ,SC_Sales from base_data fi left join (select Date, ASIN, sum(ifnull(glanceviews,0)) Glance_Views from maplemonk.KA_Amazon_Vendor_Central_Traffic_Fact_Items group by 1,2 ) AVCV on fi.Date = AVCV.Date and lower(fi.ASIN) = lower(AVCV.ASIN) left join (select MARKETPLACE_SKU skucode, name, category, category_code sub_category, category_code, commonsku, TAX_RATE from (select * from maplemonk.final_sku_master where upper(data_source) like \'AMAZON SELLER CENTRAL INDIA SESSIONS API\') qualify row_number() over (partition by lower(ifnull(trim(MARKETPLACE_SKU),\'\')) order by lower(ifnull(trim(COMMONSKU),\'\')) desc) = 1 ) p on lower(fi.ASIN) = lower(p.skucode) ;",
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
            