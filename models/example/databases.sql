{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table SELECT_DB.maplemonk.SELECT_DB_amazon_sales_cost as WITH SKUMASTER AS ( select * from (select \"Child SKU\" skucode ,\"Channel ID\" product_id ,\"Channel Name\" Channel_Name ,\"Child category\" name ,\"Channel SKU\" Channel_SKU ,\"Parent SKU\" Parent_SKU ,\"Parent category\" category ,\"Child category\" sub_category ,\"Brand Name\" Brand_Name ,\"New Parent\" New_Parent_1 ,\"New Parent 1\" New_Parent ,\"Bundle Size\" Bundle_Size ,COLOR ,MRP ,SP Selling_Price ,row_number() over (partition by \"Channel ID\", \"Channel Name\" order by 1) rw from SELECT_DB.MAPLEMONK.kyari_sku_master_main where lower(\"Channel Name\") like any (\'%amazon%\') ) where rw=1 ) , sellerCentralSales as ( select order_timestamp::date Date ,upper(shop_name) marketplace ,a.product_id ASIN ,upper(coalesce(b.name, a.product_name_final)) product_name_final ,upper(coalesce(b.category,a.product_category)) mapped_product_category ,upper(coalesce(b.sub_category,a.product_sub_category)) mapped_sub_category ,upper(b.brand_name) brand_name ,upper(b.New_Parent) New_Parent ,upper(b.New_Parent_1) New_Parent_1 ,upper(b.Bundle_Size) Bundle_Size ,upper(b.COLOR) COLOR ,count(distinct order_id) SC_Orders ,sum(ifnull(quantity,0)) SC_Units ,sum(ifnull(line_item_sales,0)) SC_Item_price_Sales ,sum(ifnull(SHIPPING_PRICE,0)) SC_Shipping_Price ,sum(ifnull(Tax,0)) TAX ,sum(ifnull(ship_promotion_discount,0)) SHIP_PROMOTION_DISCOUNT ,sum(ifnull(gift_wrap_price,0)) GIFT_WRAP_PRICE ,sum(ifnull(gift_wrap_tax,0)) GIFT_WRAP_TAX ,sum(ifnull(discount,0)) Discount ,sum(ifnull(total_sales,0)) Total_Sales from SELECT_DB.maplemonk.SELECT_DB_amazon_fact_items a left join SKUMASTER b on a.product_id = b.product_id and lower(b.channel_name) = \'amazon sc\' group by order_timestamp::date ,upper(shop_name) ,a.product_id ,upper(coalesce(b.name, a.product_name_final)) ,upper(coalesce(b.category,a.product_category)) ,upper(coalesce(b.sub_category,a.product_sub_category)) ,upper(b.brand_name) ,upper(b.New_Parent) ,upper(b.New_Parent_1) ,upper(b.Bundle_Size) ,upper(b.COLOR) order by order_timestamp::date desc ), sellerCentralSessions as ( with ASPTraffic as (select try_cast(DATAENDTIME as date) Date ,\'AMAZON_SELLER_CENTRAL_KYARI\' Marketplace ,parentasin ASIN ,sum(ifnull(trafficbyasin:\"browserPageViews\",0)) Browser_Page_Views ,sum(ifnull(trafficbyasin:\"browserSessions\",0)) Browser_Sessions ,sum(ifnull(trafficbyasin:\"buyBoxPercentage\",0)) BuyBox_Percentage ,sum(ifnull(trafficbyasin:\"mobileAppPageViews\",0)) MobileApp_Page_Views ,sum(ifnull(trafficbyasin:\"mobileAppSessions\",0)) MobileApp_Sessions ,sum(ifnull(trafficbyasin:\"pageViews\",0)) Page_Views ,sum(ifnull(trafficbyasin:\"sessions\",0)) Sessions ,sum(ifnull(sales.value:\"unitsOrdered\"::float,0)) SC_UnitsOrdered ,sum(ifnull(sales.value:\"totalOrderItems\"::float,0)) SC_ItemsOrdered ,sum(ifnull(sales.value:\"orderedProductSales\":\"amount\"::float,0)) SC_Sales from SELECT_DB.maplemonk.asp_kyari_get_sales_and_traffic_report_asin , lateral flatten (input => salesbyasin) Sales group by try_cast(DATAENDTIME as date), Marketplace, parentasin ) select a.* ,upper(b.name) Product_Name_Final ,upper(b.category) Mapped_Product_Category ,upper(b.sub_category) Mapped_Sub_Category ,upper(b.brand_name) Brand_Name ,upper(b.New_Parent) New_Parent ,upper(b.New_Parent_1) New_Parent_1 ,upper(b.Bundle_Size) Bundle_Size ,upper(b.COLOR) COLOR ,b.skucode SKU from ASPTRAFFIC a left join SKUMASTER b on a.ASIN = b.product_id and lower(b.channel_name) = \'amazon sc\' ) select coalesce(sellerCentralSales.DATE,VendorCentralSales.DATE, sellerCentralSessions.DATE, amazonads.date) Date ,coalesce(sellerCentralSales.Marketplace,VendorCentralSales.Marketplace, sellerCentralSessions.Marketplace, amazonads.Marketplace) Marketplace ,coalesce(sellerCentralSales.ASIN,VendorCentralSales.ASIN, sellerCentralSessions.ASIN, amazonads.asin) ASIN ,upper(coalesce(sellerCentralSales.Product_Name_Final,VendorCentralSales.Product_Name_Final, sellerCentralSessions.Product_Name_Final, amazonads.product_name_final)) Product_Name_Final ,upper(coalesce(sellerCentralSales.Mapped_Product_Category,VendorCentralSales.Mapped_Category, sellerCentralSessions.Mapped_Product_Category, amazonads.category)) Mapped_Product_Category ,upper(coalesce(sellerCentralSales.Mapped_Sub_Category,VendorCentralSales.Mapped_Sub_Category, sellerCentralSessions.Mapped_Sub_Category, amazonads.sub_category)) Mapped_Sub_Category ,upper(coalesce(sellerCentralSales.brand_name,VendorCentralSales.brand_name, sellerCentralSessions.brand_name, amazonads.brand_name)) brand_name ,upper(coalesce(sellerCentralSales.New_Parent,VendorCentralSales.New_Parent, sellerCentralSessions.New_Parent, amazonads.New_Parent)) New_Parent ,upper(coalesce(sellerCentralSales.New_Parent_1,VendorCentralSales.New_Parent_1, sellerCentralSessions.New_Parent_1, amazonads.New_Parent_1)) New_Parent_1 ,upper(coalesce(sellerCentralSales.Bundle_Size,VendorCentralSales.Bundle_Size, sellerCentralSessions.Bundle_Size, amazonads.Bundle_Size)) Bundle_Size ,upper(coalesce(sellerCentralSales.COLOR,VendorCentralSales.COLOR, sellerCentralSessions.COLOR, amazonads.COLOR)) COLOR ,ifnull(sellerCentralSales.SC_Orders,0) SC_Orders ,ifnull(sellerCentralSales.SC_Units,0) SC_Units ,ifnull(sellerCentralSales.Total_Sales,0) SC_Sales ,ifnull(sellerCentralSales.Discount,0) SC_Discount ,ifnull(VendorCentralSales.Shipped_Units,0) VC_Shipped_Units ,ifnull(VendorCentralSales.Customer_returns,0) VC_Customer_returns_units ,ifnull(VendorCentralSales.shipped_revenue_final,0) VC_shipped_revenue ,ifnull(VendorCentralSales.returned_revenue_final,0) VC_returned_revenue ,ifnull(VendorCentralSales.Shipped_Units,0)-ifnull(VendorCentralSales.Customer_returns,0) VC_Units ,ifnull(VendorCentralSales.shipped_revenue_final,0)-ifnull(VendorCentralSales.returned_revenue_final,0) VC_Sales ,ifnull(VendorCentralSales.GLANCE_VIEWS,0) VC_GLANCE_VIEWS ,ifnull(sellerCentralSessions.Sessions,0) SC_Sessions ,ifnull(sellerCentralSessions.Page_Views,0) SC_Page_Views ,ifnull(sellerCentralSessions.SC_UnitsOrdered,0) SC_UnitsOrdered_Traffic_report ,ifnull(sellerCentralSessions.SC_Sales,0) SC_Sales_Traffic_Report ,ifnull(sellerCentralSessions.Browser_Sessions,0) SC_Browser_Sessions ,ifnull(sellerCentralSessions.Browser_Page_Views,0) SC_Browser_Page_Views ,ifnull(sellerCentralSessions.MobileApp_Page_Views,0) SC_MobileApp_Page_Views ,ifnull(sellerCentralSessions.MobileApp_Sessions,0) SC_MobileApp_Sessions ,ifnull(sellerCentralSessions.BuyBox_Percentage,0) SC_BuyBox_Percentage ,ifnull(amazonads.total_ad_spend_session_share,0) TOTAL_AD_SPEND_SESSION_SHARE ,ifnull(amazonads.total_adsales_session_share,0) TOTAL_AD_SALES_SESSION_SHARE ,ifnull(amazonads.total_impressions_session_share,0) TOTAL_IMPRESSIONS_SESSION_SHARE ,ifnull(amazonads.total_clicks_session_share,0) TOTAL_CLICKS_SESSION_SHARE ,ifnull(amazonads.total_ad_conversions_session_share,0) TOTAL_AD_CONVERSIONS_SESSION_SHARE ,ifnull(amazonads.sd_ad_spend,0) DISPLAY_AD_SPEND ,ifnull(amazonads.sp_ad_spend,0) PRODUCTS_AD_SPEND ,ifnull(amazonads.sb_spend_session_share,0) BRAND_AD_SPEND_SESSION_SHARE ,ifnull(amazonads.sd_impressions,0) DISPLAY_IMPRESSIONS ,ifnull(amazonads.sp_impressions,0) PRODUCTS_IMPRESSIONS ,ifnull(amazonads.sb_impression_session_share,0) BRAND_IMPRESSIONS ,ifnull(amazonads.SD_CLICKS,0) DISPLAY_CLICKS ,ifnull(amazonads.sp_clicks,0) PRODUCTS_CLICKS ,ifnull(amazonads.sb_clicks_session_share,0) BRAND_CLICKS ,ifnull(amazonads.sd_ad_conversions,0) DISPLAY_AD_CONVERSIONS ,ifnull(amazonads.sp_ad_conversions,0) PRODUCTS_AD_CONVERSIONS ,ifnull(amazonads.sb_ad_conversions_session_share,0) BRAND_AD_CONVERSIONS ,ifnull(amazonads.sd_adsales,0) DISPLAY_AD_SALES ,ifnull(amazonads.SP_ADSALES,0) PRODUCTS_AD_SALES ,ifnull(amazonads.sb_ad_sales_session_share,0) BRAND_AD_SALES from sellerCentralSales full outer join SELECT_DB.maplemonk.SELECT_DB_AVP_FACTITEMS VendorCentralSales on sellerCentralSales.ASIN = VendorCentralSales.ASIN and sellerCentralSales.date=VendorCentralSales.date and lower(sellerCentralSales.marketplace) = lower(VendorCentralSales.marketplace) full outer join sellerCentralSessions on coalesce(sellerCentralSales.ASIN,VendorCentralSales.ASIN) = sellerCentralSessions.ASIN and coalesce(sellerCentralSales.date,VendorCentralSales.date) = sellerCentralSessions.date and coalesce(sellerCentralSales.marketplace,VendorCentralSales.marketplace) = sellerCentralSessions.marketplace full outer join SELECT_DB.maplemonk.SELECT_DB_AMAZONADS_CONSOLIDATED amazonads on coalesce(sellerCentralSales.ASIN,VendorCentralSales.ASIN,sellerCentralSessions.ASIN) = amazonads.ASIN and coalesce(sellerCentralSales.date,VendorCentralSales.date,sellerCentralSessions.date) = amazonads.date and coalesce(sellerCentralSales.marketplace,VendorCentralSales.marketplace,sellerCentralSessions.marketplace) = amazonads.marketplace;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SELECT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        