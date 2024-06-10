{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table XYXX_DB.maplemonk.XYXX_DB_amazon_sales_cost as select coalesce(VendorCentralSales.DATE, amazonads.date) Date ,\'AMAZON\' Marketplace ,coalesce(VendorCentralSales.ASIN, amazonads.asin) ASIN ,upper(coalesce(VendorCentralSales.Product_Name_Final, amazonads.product_name_final)) Product_Name_Final ,upper(coalesce(VendorCentralSales.Mapped_Category, amazonads.category)) Mapped_Product_Category ,upper(coalesce(VendorCentralSales.Mapped_Sub_Category, amazonads.sub_category)) Mapped_Sub_Category ,ifnull(VendorCentralSales.Shipped_Units,0) VC_Shipped_Units ,ifnull(VendorCentralSales.Customer_returns,0) VC_Customer_returns_units ,ifnull(VendorCentralSales.shipped_revenue_final,0) VC_shipped_revenue ,ifnull(VendorCentralSales.returned_revenue_final,0) VC_returned_revenue ,ifnull(VendorCentralSales.Shipped_Units,0)-ifnull(VendorCentralSales.Customer_returns,0) VC_Units ,ifnull(VendorCentralSales.shipped_revenue_final,0)-ifnull(VendorCentralSales.returned_revenue_final,0) VC_Sales ,ifnull(VendorCentralSales.GLANCE_VIEWS,0) VC_GLANCE_VIEWS ,ifnull(amazonads.total_ad_spend_session_share,0) TOTAL_AD_SPEND_SESSION_SHARE ,ifnull(amazonads.total_adsales_session_share,0) TOTAL_AD_SALES_SESSION_SHARE ,ifnull(amazonads.total_impressions_session_share,0) TOTAL_IMPRESSIONS_SESSION_SHARE ,ifnull(amazonads.total_clicks_session_share,0) TOTAL_CLICKS_SESSION_SHARE ,ifnull(amazonads.total_ad_conversions_session_share,0) TOTAL_AD_CONVERSIONS_SESSION_SHARE ,ifnull(amazonads.sd_ad_spend,0) DISPLAY_AD_SPEND ,ifnull(amazonads.sp_ad_spend,0) PRODUCTS_AD_SPEND ,ifnull(amazonads.sb_spend_session_share,0) BRAND_AD_SPEND_SESSION_SHARE ,ifnull(amazonads.sd_impressions,0) DISPLAY_IMPRESSIONS ,ifnull(amazonads.sp_impressions,0) PRODUCTS_IMPRESSIONS ,ifnull(amazonads.sb_impression_session_share,0) BRAND_IMPRESSIONS ,ifnull(amazonads.SD_CLICKS,0) DISPLAY_CLICKS ,ifnull(amazonads.sp_clicks,0) PRODUCTS_CLICKS ,ifnull(amazonads.sb_clicks_session_share,0) BRAND_CLICKS ,ifnull(amazonads.sd_ad_conversions,0) DISPLAY_AD_CONVERSIONS ,ifnull(amazonads.sp_ad_conversions,0) PRODUCTS_AD_CONVERSIONS ,ifnull(amazonads.sb_ad_conversions_session_share,0) BRAND_AD_CONVERSIONS ,ifnull(amazonads.sd_adsales,0) DISPLAY_AD_SALES ,ifnull(amazonads.SP_ADSALES,0) PRODUCTS_AD_SALES ,ifnull(amazonads.sb_ad_sales_session_share,0) BRAND_AD_SALES from XYXX_DB.maplemonk.XYXX_DB_AVP_FACTITEMS VendorCentralSales full outer join XYXX_DB.maplemonk.XYXX_DB_AMAZONADS_CONSOLIDATED amazonads on VendorCentralSales.ASIN = amazonads.ASIN and VendorCentralSales.date = amazonads.date;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from XYXX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        