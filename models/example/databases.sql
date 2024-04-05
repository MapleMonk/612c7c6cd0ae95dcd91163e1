{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table SELECT_DB.MAPLEMONK.SELECT_DB_amazonads_consolidated as with SKUMASTER AS ( select * from (select \"Child SKU\" skucode ,\"Channel ID\" product_id ,\"Channel Name\" Channel_Name ,\"Child category\" name ,\"Channel SKU\" Channel_SKU ,\"Parent SKU\" Parent_SKU ,\"Parent category\" category ,\"Child category\" sub_category ,\"Brand Name\" Brand_Name ,upper(\"New Parent 1\") New_Parent ,\"New Parent\" New_Parent_1 ,\"Bundle Size\" Bundle_Size ,Upper(state) STATE ,TITLE ,upper(color) COLOR ,MRP ,SP Selling_Price ,row_number() over (partition by \"Channel ID\" order by 1) rw from SELECT_DB.MAPLEMONK.kyari_sku_master_main where lower(\"Channel Name\") like any (\'%amazon vc%\', \'%amazon sc%\') ) where rw=1 ), AdCampaignASINMap as ( select * ,count(ASIN) over (partition by lower(campaign) order by 1) ASIN_COUNT from (select * from (Select campaigns campaign ,upper(\"New Parent\") category ,upper(\"Child Category\") sub_category ,ASIN ,row_number() over (partition by lower(campaigns), lower(ASIN) order by 1) rw from SELECT_DB.MAPLEMONK.select_amazon_final_sb_data ) where rw=1 and ASIN <> \'#N/A\' ) ), SPSDASIN as ( select DATE ,ASIN ,sum(case when lower(CAMPAIGN_TYPE) like \'%display%\' then ifnull(impressions,0) end) SD_IMPRESSIONS ,sum(case when lower(CAMPAIGN_TYPE) like \'%product%\' then ifnull(impressions,0) end) SP_IMPRESSIONS ,sum(case when lower(CAMPAIGN_TYPE) like \'%display%\' then ifnull(clicks,0) end) SD_CLICKS ,sum(case when lower(CAMPAIGN_TYPE) like \'%product%\' then ifnull(clicks,0) end) SP_CLICKS ,sum(case when lower(CAMPAIGN_TYPE) like \'%display%\' then ifnull(conversions,0) end) SD_AD_conversions ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(conversions,0) END) AS SP_AD_conversions ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(spend,0) END) AS SD_AD_spend ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(spend,0) END) AS SP_AD_spend ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(adsales,0) END) AS SD_adsales ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(adsales,0) END) AS SP_adsales ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(conversionssamesku,0) END) AS SD_AD_conversionssamesku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(conversionssamesku,0) END) AS SP_AD_conversionssamesku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(conversionsothersku,0) END) AS SD_AD_conversionsothersku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(conversionsothersku,0) END) AS SP_AD_conversionsothersku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(salessamesku,0) END) AS SD_AD_salessamesku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(salessamesku,0) END) AS SP_AD_salessamesku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(OTHERSKUSALES,0) END) AS SD_AD_OTHERSKUSALES ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(OTHERSKUSALES,0) END) AS SP_AD_OTHERSKUSALES ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(newtobrandsales,0) END) AS SD_AD_newtobrandsales ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(newtobrandsales,0) END) AS SP_AD_newtobrandsales ,sum(case when lower(CAMPAIGN_TYPE) like \'%display%\' then ifnull(newtobrandunits,0) end) as SD_AD_newtobrandunits ,sum(case when lower(CAMPAIGN_TYPE) like \'%product%\' then ifnull(newtobrandunits,0) end) as SP_AD_newtobrandunits ,sum(case when lower(CAMPAIGN_TYPE) like \'%display%\' then ifnull(newtobrandorders,0) end) as SD_AD_newtobrandorders ,sum(case when lower(CAMPAIGN_TYPE) like \'%product%\' then ifnull(newtobrandorders,0) end) as SP_AD_newtobrandorders from SELECT_DB.MAPLEMONK.SELECT_DB_amazonads_marketing where lower(campaign_type) like any (\'%products%\', \'%display%\') group by DATE ,ASIN ), SBSBVCampaigns as (select Date ,ASIN ,sum(IMPRESSIONS) IMPRESSIONS ,sum(CLICKS) CLICKS ,sum(Ad_CONVERSIONS) Ad_CONVERSIONS ,sum(SPEND) SPEND ,sum(AD_Sales) AD_Sales ,sum(Ad_CONVERSIONSSAMESKU) Ad_CONVERSIONSSAMESKU ,sum(AD_CONVERSIONOTHERSKU) AD_CONVERSIONOTHERSKU ,sum(AD_SalesSAMESKU) AD_SalesSAMESKU ,sum(AD_SalesOTHERSKU) AD_SalesOTHERSKU ,sum(AD_NEWTOBRAND_ORDERS) AD_NEWTOBRAND_ORDERS ,sum(AD_NEWTOBRAND_SALES) AD_NEWTOBRAND_SALES ,sum(AD_NEWTOBRAND_UNITS) AD_NEWTOBRAND_UNITS from ( select A.DATE ,A.CAMPAIGN_TYPE ,Upper(A.campaignname) campaignname ,B.ASIN ,max(B.ASIN_COUNT) ASIN_COUNT ,div0(sum(ifnull(impressions,0)),count(1) over (partition by upper(A.campaignname), A.DATE order by 1)) IMPRESSIONS ,div0(sum(ifnull(clicks,0)),count(1) over (partition by upper(A.campaignname), A.DATE order by 1)) CLICKS ,div0(sum(ifnull(conversions,0)),count(1) over (partition by upper(A.campaignname), A.DATE order by 1)) Ad_CONVERSIONS ,div0(sum(ifnull(SPEND,0)),count(1) over (partition by upper(A.campaignname), A.DATE order by 1)) SPEND ,div0(sum(ifnull(adsales,0)),count(1) over (partition by upper(A.campaignname), A.DATE order by 1)) AD_Sales ,div0(sum(ifnull(conversionssamesku,0)),count(1) over (partition by upper(A.campaignname), A.DATE order by 1)) Ad_CONVERSIONSSAMESKU ,div0(sum(ifnull(conversionsothersku,0)),count(1) over (partition by upper(A.campaignname), A.DATE order by 1)) AD_CONVERSIONOTHERSKU ,div0(sum(ifnull(salessamesku,0)),count(1) over (partition by upper(A.campaignname), A.DATE order by 1)) AD_SalesSAMESKU ,div0(sum(ifnull(OTHERSKUSALES,0)),count(1) over (partition by upper(A.campaignname), A.DATE order by 1)) AD_SalesOTHERSKU ,div0(sum(ifnull(newtobrandorders,0)),count(1) over (partition by upper(A.campaignname), A.DATE order by 1)) AD_NEWTOBRAND_ORDERS ,div0(sum(ifnull(newtobrandsales,0)),count(1) over (partition by upper(A.campaignname), A.DATE order by 1)) AD_NEWTOBRAND_SALES ,div0(sum(ifnull(newtobrandunits,0)),count(1) over (partition by upper(A.campaignname), A.DATE order by 1)) AD_NEWTOBRAND_UNITS from SELECT_DB.MAPLEMONK.SELECT_DB_amazonads_marketing A left join AdCampaignASINMap B on lower(A.campaignname) = lower(B.campaign) where lower(A.campaign_type) like any (\'%brand%\') group by A.DATE ,A.CAMPAIGN_TYPE ,upper(A.campaignname) ,B.ASIN ) group by date, asin ) select coalesce(A.Date, B.Date) Date ,coalesce(A.ASIN, B.ASIN) ASIN ,\'AMAZON_SELLER_CENTRAL_KYARI\' Marketplace ,C.Name Product_Name_Final ,C.Category Category ,C.Sub_category Sub_Category ,C.brand_name brand_name ,C.Parent_SKU ,C.skucode CHILD_SKU ,C.New_Parent_1 ,C.New_Parent ,C.Bundle_Size ,C.State ,C.color ,C.Title ,SD_IMPRESSIONS ,SP_IMPRESSIONS ,SD_CLICKS ,SP_CLICKS ,SD_AD_conversions ,SP_AD_conversions ,SD_AD_spend ,SP_AD_spend ,SD_adsales ,SP_adsales ,SD_AD_conversionssamesku ,SP_AD_conversionssamesku ,SD_AD_conversionsothersku ,SP_AD_conversionsothersku ,SD_AD_salessamesku ,SP_AD_salessamesku ,SD_AD_OTHERSKUSALES ,SP_AD_OTHERSKUSALES ,SD_AD_newtobrandsales ,SP_AD_newtobrandsales ,SD_AD_newtobrandunits ,SP_AD_newtobrandunits ,SD_AD_newtobrandorders ,SP_AD_newtobrandorders ,B.Impressions SB_Impression_Session_Share ,B.CLICKS SB_Clicks_Session_Share ,B.Ad_CONVERSIONS SB_Ad_Conversions_Session_Share ,B.SPEND SB_Spend_Session_Share ,B.AD_Sales SB_Ad_Sales_Session_Share ,B.Ad_CONVERSIONSSAMESKU SB_Ad_Conversions_Same_SKU_Session_Share ,B.AD_CONVERSIONOTHERSKU SB_ConversionOtherSKU_Session_Share ,B.AD_SalesSAMESKU SB_Sales_SameSKU_Session_Share ,B.AD_SalesOTHERSKU SB_Sales_OtherSKU_Session_Share ,B.AD_NEWTOBRAND_ORDERS SB_AD_NEWTOBRAND_ORDERS_Session_Share ,B.AD_NEWTOBRAND_SALES SB_AD_NEWTOBRAND_SALES_Session_Share ,B.AD_NEWTOBRAND_UNITS SB_AD_NEWTOBRAND_UNITS_Session_Share ,(ifnull(SB_Impression_Session_Share,0) + ifnull(SD_IMPRESSIONS,0) + ifnull(SP_IMPRESSIONS,0)) as Total_IMPRESSIONS_Session_Share ,(ifnull(SB_Clicks_Session_Share,0) + ifnull(SD_CLICKS,0) + ifnull(SP_CLICKS,0)) as Total_CLICKS_Session_Share ,(ifnull(SB_Ad_Conversions_Session_Share,0) + ifnull(SD_AD_conversions,0) + ifnull(SP_AD_conversions,0)) as Total_AD_conversions_Session_Share ,(ifnull(SB_Spend_Session_Share,0) + ifnull(SD_AD_spend,0) + ifnull(SP_AD_spend,0)) as Total_AD_spend_Session_Share ,(ifnull(SB_Ad_Sales_Session_Share,0) + ifnull(SD_adsales,0) + ifnull(SP_adsales,0)) as Total_adsales_Session_Share ,(ifnull(SB_Ad_Conversions_Same_SKU_Session_Share,0) + ifnull(SD_AD_conversionssamesku,0) + ifnull(SP_AD_conversionssamesku,0)) as Total_AD_conversionssamesku_Session_Share ,(ifnull(SB_ConversionOtherSKU_Session_Share,0) + ifnull(SD_AD_conversionsothersku,0) + ifnull(SP_AD_conversionsothersku,0)) as Total_AD_conversionsothersku_Session_Share ,(ifnull(SB_Sales_SameSKU_Session_Share,0) + ifnull(SD_AD_salessamesku,0) + ifnull(SP_AD_salessamesku,0)) as Total_AD_salessamesku_Session_Share ,(ifnull(SB_Sales_OtherSKU_Session_Share,0) + ifnull(SD_AD_OTHERSKUSALES,0) + ifnull(SP_AD_OTHERSKUSALES,0)) as Total_AD_OTHERSKUSALES_Session_Share ,(ifnull(SB_AD_NEWTOBRAND_ORDERS_Session_Share,0) + ifnull(SD_AD_newtobrandsales,0) + ifnull(SP_AD_newtobrandsales,0)) as Total_AD_newtobrandsales_Session_Share ,(ifnull(SB_AD_NEWTOBRAND_SALES_Session_Share,0) + ifnull(SD_AD_newtobrandunits,0) + ifnull(SP_AD_newtobrandunits,0)) as Total_AD_newtobrandunits_Session_Share ,(ifnull(SB_AD_NEWTOBRAND_UNITS_Session_Share,0) + ifnull(SD_AD_newtobrandorders,0) + ifnull(SP_AD_newtobrandorders,0)) as Total_AD_newtobrandorders_Session_Share from SPSDASIN A full outer join SBSBVCampaigns B on A.Date=B.Date and A.ASIN = B.ASIN left join SKUMASTER C on coalesce(A.ASIN, B.ASIN) = C.product_id ;",
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
                        