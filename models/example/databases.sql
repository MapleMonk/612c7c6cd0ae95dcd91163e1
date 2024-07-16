{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE EMMASLEEP_DB.MAPLEMONK.ASP_BR_TRAFFIC_FACT_ITEMS AS select try_cast(DATAENDTIME as date) Date ,\'AMAZON_SLP\' Marketplace ,parentasin ASIN ,sum(ifnull(trafficbyasin:\"browserPageViews\",0)) Browser_Page_Views ,sum(ifnull(trafficbyasin:\"browserSessions\",0)) Browser_Sessions ,sum(ifnull(trafficbyasin:\"buyBoxPercentage\",0)) BuyBox_Percentage ,sum(ifnull(trafficbyasin:\"mobileAppPageViews\",0)) MobileApp_Page_Views ,sum(ifnull(trafficbyasin:\"mobileAppSessions\",0)) MobileApp_Sessions ,sum(ifnull(trafficbyasin:\"pageViews\",0)) Page_Views ,sum(ifnull(trafficbyasin:\"sessions\",0)) Sessions ,sum(ifnull(salesbyasin:\"unitsOrdered\"::float,0)) SC_UnitsOrdered ,sum(ifnull(salesbyasin:\"totalOrderItems\"::float,0)) SC_ItemsOrdered ,sum(ifnull(salesbyasin:\"orderedProductSales\":\"amount\"::float,0)) SC_Sales from EMMASLEEP_DB.MAPLEMONK.Amazon_Seller_partner_Business_Reports_ASP_BR_Emma_India_GET_SALES_AND_TRAFFIC_REPORT_ASIN group by try_cast(DATAENDTIME as date), Marketplace, parentasin ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EMMASLEEP_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        