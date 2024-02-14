{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_AMAZON_SALES_COST as with SKUMASTER AS ( select marketplace_sku product_id ,name ,category ,sub_category ,skucode common_sku_code from SLEEPYCAT_DB.MAPLEMONK.FINAL_SKU_MASTER ), sellerCentralSales as ( select order_timestamp::date Date ,upper(shop_name) marketplace ,a.product_id ASIN ,count(distinct order_id) SC_Orders ,sum(ifnull(quantity,0)) SC_Units ,sum(ifnull(line_item_sales,0)) SC_Item_price_Sales ,sum(ifnull(SHIPPING_PRICE,0)) SC_Shipping_Price ,sum(ifnull(Tax,0)) TAX ,sum(ifnull(ship_promotion_discount,0)) SHIP_PROMOTION_DISCOUNT ,sum(ifnull(gift_wrap_price,0)) GIFT_WRAP_PRICE ,sum(ifnull(gift_wrap_tax,0)) GIFT_WRAP_TAX ,sum(ifnull(discount,0)) Discount ,sum(ifnull(total_sales,0)) Total_Sales from SLEEPYCAT_DB.maplemonk.SLEEPYCAT_DB_amazon_fact_items a where lower(order_status) not in (\'cancelled\') group by order_timestamp::date ,upper(shop_name) ,a.product_id order by order_timestamp::date desc ), sellerCentralSessions as ( select try_cast(DATAENDTIME as date) Date ,\'AMAZON_SLP\' Marketplace ,parentasin ASIN ,sum(ifnull(trafficbyasin:\"browserPageViews\",0)) Browser_Page_Views ,sum(ifnull(trafficbyasin:\"browserSessions\",0)) Browser_Sessions ,sum(ifnull(trafficbyasin:\"buyBoxPercentage\",0)) BuyBox_Percentage ,sum(ifnull(trafficbyasin:\"mobileAppPageViews\",0)) MobileApp_Page_Views ,sum(ifnull(trafficbyasin:\"mobileAppSessions\",0)) MobileApp_Sessions ,sum(ifnull(trafficbyasin:\"pageViews\",0)) Page_Views ,sum(ifnull(trafficbyasin:\"sessions\",0)) Sessions ,sum(ifnull(sales.value:\"unitsOrdered\"::float,0)) SC_UnitsOrdered ,sum(ifnull(sales.value:\"totalOrderItems\"::float,0)) SC_ItemsOrdered ,sum(ifnull(sales.value:\"orderedProductSales\":\"amount\"::float,0)) SC_Sales from SLEEPYCAT_DB.maplemonk.ASP_BR_SC_GET_SALES_AND_TRAFFIC_REPORT_ASIN , lateral flatten (input => salesbyasin) Sales group by try_cast(DATAENDTIME as date), Marketplace, parentasin ) select coalesce(sellerCentralSales.DATE, sellerCentralSessions.DATE, amazonads.date) Date ,coalesce(sellerCentralSales.Marketplace, sellerCentralSessions.Marketplace, amazonads.Marketplace) Marketplace ,coalesce(sellerCentralSales.ASIN, sellerCentralSessions.ASIN, amazonads.asin) ASIN ,SKUMASTER.common_sku_code ,upper(SKUMASTER.NAME) Product_Name_Final ,upper(SKUMASTER.CATEGORY) Mapped_Product_Category ,upper(SKUMASTER.SUB_CATEGORY) Mapped_Sub_Category ,ifnull(sellerCentralSales.SC_Orders,0) SC_Orders ,ifnull(sellerCentralSales.SC_Units,0) SC_Units ,ifnull(sellerCentralSales.Total_Sales,0) SC_Sales ,ifnull(sellerCentralSales.Discount,0) SC_Discount ,ifnull(sellerCentralSessions.Sessions,0) SC_Sessions ,ifnull(sellerCentralSessions.Page_Views,0) SC_Page_Views ,ifnull(sellerCentralSessions.SC_UnitsOrdered,0) SC_UnitsOrdered_Traffic_report ,ifnull(sellerCentralSessions.SC_Sales,0) SC_Sales_Traffic_Report ,ifnull(sellerCentralSessions.Browser_Sessions,0) SC_Browser_Sessions ,ifnull(sellerCentralSessions.Browser_Page_Views,0) SC_Browser_Page_Views ,ifnull(sellerCentralSessions.MobileApp_Page_Views,0) SC_MobileApp_Page_Views ,ifnull(sellerCentralSessions.MobileApp_Sessions,0) SC_MobileApp_Sessions ,ifnull(sellerCentralSessions.BuyBox_Percentage,0) SC_BuyBox_Percentage ,ifnull(amazonads.total_ad_spend_session_share,0) TOTAL_AD_SPEND_SESSION_SHARE ,ifnull(amazonads.total_adsales_session_share,0) TOTAL_AD_SALES_SESSION_SHARE ,ifnull(amazonads.total_impressions_session_share,0) TOTAL_IMPRESSIONS_SESSION_SHARE ,ifnull(amazonads.total_clicks_session_share,0) TOTAL_CLICKS_SESSION_SHARE ,ifnull(amazonads.total_ad_conversions_session_share,0) TOTAL_AD_CONVERSIONS_SESSION_SHARE ,ifnull(amazonads.sd_ad_spend,0) DISPLAY_AD_SPEND ,ifnull(amazonads.sp_ad_spend,0) PRODUCTS_AD_SPEND ,ifnull(amazonads.sb_spend_session_share,0) BRAND_AD_SPEND_SESSION_SHARE ,ifnull(amazonads.sd_impressions,0) DISPLAY_IMPRESSIONS ,ifnull(amazonads.sp_impressions,0) PRODUCTS_IMPRESSIONS ,ifnull(amazonads.sb_impression_session_share,0) BRAND_IMPRESSIONS ,ifnull(amazonads.SD_CLICKS,0) DISPLAY_CLICKS ,ifnull(amazonads.sp_clicks,0) PRODUCTS_CLICKS ,ifnull(amazonads.sb_clicks_session_share,0) BRAND_CLICKS ,ifnull(amazonads.sd_ad_conversions,0) DISPLAY_AD_CONVERSIONS ,ifnull(amazonads.sp_ad_conversions,0) PRODUCTS_AD_CONVERSIONS ,ifnull(amazonads.sb_ad_conversions_session_share,0) BRAND_AD_CONVERSIONS ,ifnull(amazonads.sd_adsales,0) DISPLAY_AD_SALES ,ifnull(amazonads.SP_ADSALES,0) PRODUCTS_AD_SALES ,ifnull(amazonads.sb_ad_sales_session_share,0) BRAND_AD_SALES from sellerCentralSales full outer join sellerCentralSessions on sellerCentralSales.ASIN = sellerCentralSessions.ASIN and sellerCentralSales.date = sellerCentralSessions.date full outer join SLEEPYCAT_DB.maplemonk.SLEEPYCAT_DB_AMAZONADS_CONSOLIDATED amazonads on coalesce(sellerCentralSales.ASIN,sellerCentralSessions.ASIN) = amazonads.ASIN and coalesce(sellerCentralSales.date,sellerCentralSessions.date) = amazonads.date left join SKUMASTER on coalesce(sellerCentralSales.ASIN,sellerCentralSessions.ASIN, amazonads.ASIN) = SKUMASTER.product_id;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SLEEPYCAT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        