{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table medmongers_db.maplemonk.medmongers_amazon_traffic_report as select a.parentasin, s.brand, \'AMAZON MEDMONGERS\' as marketplace, \'AMAZON MEDMONGERS\' as marketing_channel, to_date(dataendtime) as date, sum(ifnull(trafficbyasin:\"browserPageViews\",0)) Browser_Page_Views, sum(ifnull(trafficbyasin:\"browserSessions\",0)) Browser_Sessions, sum(ifnull(trafficbyasin:\"buyBoxPercentage\",0)) BuyBox_Percentage, sum(ifnull(trafficbyasin:\"mobileAppPageViews\",0)) MobileApp_Page_Views, sum(ifnull(trafficbyasin:\"mobileAppSessions\",0)) MobileApp_Sessions, sum(ifnull(trafficbyasin:\"pageViews\",0)) Page_Views, sum(ifnull(trafficbyasin:\"sessions\",0)) Sessions, sum(ifnull(salesbyasin:\"unitsOrdered\"::float,0)) SC_UnitsOrdered, sum(ifnull(salesbyasin:\"totalOrderItems\"::float,0)) SC_ItemsOrdered, sum(ifnull(salesbyasin:\"orderedProductSales\":\"amount\"::float,0)) SC_Sales from medmongers_db.maplemonk.amazon_br_medmongers_get_sales_and_traffic_report_asin a left join (select product_id, brand, from medmongers_db.maplemonk.MEDMONGERS_FINAL_SKU_MASTER where final_marketplace = \'AMAZON\' qualify row_number() over (partition by upper(product_id) order by 1)=1 ) s on lower(s.product_id) = lower(a.parentasin) group by 1,2,3,4,5 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from MEDMONGERS_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            