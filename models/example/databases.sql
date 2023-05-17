{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table buildskill148_db.maplemonk.amazon_sales_cost_buildskill as select coalesce(VendorCentralSales.DATE, amazonads.date) Date ,coalesce(VendorCentralSales.ASIN, amazonads.asin) ASIN ,upper(coalesce(VendorCentralSales.Product_Name_Final, amazonads.product_name_final)) Product_Name_Final ,upper(coalesce(VendorCentralSales.Mapped_Category, amazonads.category)) Mapped_Product_Category ,upper(coalesce(VendorCentralSales.Mapped_Sub_Category, amazonads.sub_category)) Mapped_Sub_Category ,ifnull(VendorCentralSales.Shipped_Units,0) VC_Shipped_Units ,ifnull(VendorCentralSales.Customer_returns,0) VC_Customer_returns_units ,ifnull(VendorCentralSales.shipped_revenue,0) VC_shipped_revenue ,ifnull(VendorCentralSales.returned_revenue,0) VC_returned_revenue ,case when lower(data_stream) = \'manufacturing_retail\' then ifnull(VendorCentralSales.ordered_Units,0)-ifnull(VendorCentralSales.Customer_returns,0) else ifnull(VendorCentralSales.shipped_Units,0)-ifnull(VendorCentralSales.Customer_returns,0) end VC_Units ,case when lower(data_stream) = \'manufacturing_retail\' then ifnull(VendorCentralSales.ordered_revenue,0)-ifnull(VendorCentralSales.returned_Revenue,0) else ifnull(VendorCentralSales.shipped_Units,0)- ifnull(VendorCentralSales.returned_Revenue,0) end VC_Sales ,ifnull(VendorCentralSales.GLANCE_VIEWS,0) VC_GLANCE_VIEWS ,ifnull(amazonads.sd_ad_spend,0) DISPLAY_AD_SPEND ,ifnull(amazonads.sp_ad_spend,0) PRODUCTS_AD_SPEND ,ifnull(amazonads.sd_impressions,0) DISPLAY_IMPRESSIONS ,ifnull(amazonads.sp_impressions,0) PRODUCTS_IMPRESSIONS ,ifnull(amazonads.SD_CLICKS,0) DISPLAY_CLICKS ,ifnull(amazonads.sp_clicks,0) PRODUCTS_CLICKS ,ifnull(amazonads.sd_ad_conversions,0) DISPLAY_AD_CONVERSIONS ,ifnull(amazonads.sp_ad_conversions,0) PRODUCTS_AD_CONVERSIONS ,ifnull(amazonads.sd_adsales,0) DISPLAY_AD_SALES ,ifnull(amazonads.SP_ADSALES,0) PRODUCTS_AD_SALES from buildskill148_db.maplemonk.buildskill_avp_factitems VendorCentralSales full outer join buildskill148_db.maplemonk.buildskill_amazonads_consolidated amazonads on VendorCentralSales.ASIN = amazonads.ASIN and VendorCentralSales.date = amazonads.date;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from BuildSkill148_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        