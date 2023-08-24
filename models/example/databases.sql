{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table GLADFUL_DB.MAPLEMONK.GLADFUL_amazonads_consolidated as with SKUMASTER AS ( select * from ( select a.merchant_skucode ,upper(a.marketplace) MARKETPLACE ,replace(a.skucode,\'`\',\'\') skucode ,replace(a.marketplace_product_id,\'`\',\'\') marketplace_product_id ,upper(b.name) NAME ,upper(b.category) CATEGORY ,upper(b.sub_category) SUB_CATEGORY ,row_number() over (partition by a.marketplace_product_id order by 1) rw from amazon_sku_mapping a left join (select * from (select * from (select sku skucode, product_name name, category_name category, null as sub_category, row_number() over (partition by sku order by 1) rw from gladful_db.maplemonk.easyecom_product_master) where rw = 1 ))b on replace(a.skucode, \'`\',\'\') = replace(b.skucode, \'`\',\'\') ) where rw=1 ), Sessions as ( with ASPTraffic as (select dataendtime Date ,parentasin ASIN ,sum(ifnull(trafficbyasin:\"browserPageViews\",0)) Browser_Page_Views ,sum(ifnull(trafficbyasin:\"browserSessions\",0)) Browser_Sessions ,sum(ifnull(trafficbyasin:\"buyBoxPercentage\",0)) BuyBox_Percentage ,sum(ifnull(trafficbyasin:\"mobileAppPageViews\",0)) MobileApp_Page_Views ,sum(ifnull(trafficbyasin:\"mobileAppSessions\",0)) MobileApp_Sessions ,sum(ifnull(trafficbyasin:\"pageViews\",0)) Page_Views ,sum(ifnull(trafficbyasin:\"sessions\",0)) Sessions ,sum(ifnull(salesbyasin:\"unitsOrdered\"::float,0)) SC_UnitsOrdered ,sum(ifnull(salesbyasin:\"totalOrderItems\"::float,0)) SC_ItemsOrdered ,sum(ifnull(salesbyasin:\"orderedProductSales\":\"amount\"::float,0)) SC_Sales from GLADFUL_DB.MAPLEMONK.GLADFUL_ASP_GET_SALES_AND_TRAFFIC_REPORT_ASIN group by dataendtime, parentasin ) select ASPTRAFFIC.* ,SKUMASTER.name Product_Name_Final ,SKUMASTER.category Mapped_Product_Category ,SKUMASTER.sub_category Mapped_Sub_Category ,SKUMASTER.skucode SKU from ASPTRAFFIC left join SKUMASTER on ASPTRAFFIC.ASIN=SKUMASTER.MARKETPLACE_Product_ID ), AdCampaignASINMap as ( select * from (Select campaign ,category ,sub_category ,ASIN ,row_number() over (partition by lower(campaign), lower(ASIN) order by 1) rw from GLADFUL_DB.MAPLEMONK.amazon_brand_campaign_mapping ) where rw=1 ), SPSDASIN as ( select DATE ,ASIN ,sum(case when lower(CAMPAIGN_TYPE) like \'%display%\' then ifnull(impressions,0) end) SD_IMPRESSIONS ,sum(case when lower(CAMPAIGN_TYPE) like \'%product%\' then ifnull(impressions,0) end) SP_IMPRESSIONS ,sum(case when lower(CAMPAIGN_TYPE) like \'%display%\' then ifnull(clicks,0) end) SD_CLICKS ,sum(case when lower(CAMPAIGN_TYPE) like \'%product%\' then ifnull(clicks,0) end) SP_CLICKS ,sum(case when lower(CAMPAIGN_TYPE) like \'%display%\' then ifnull(conversions,0) end) SD_AD_conversions ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(conversions,0) END) AS SP_AD_conversions ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(spend,0) END) AS SD_AD_spend ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(spend,0) END) AS SP_AD_spend ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(adsales,0) END) AS SD_adsales ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(adsales,0) END) AS SP_adsales ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(conversionssamesku,0) END) AS SD_AD_conversionssamesku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(conversionssamesku,0) END) AS SP_AD_conversionssamesku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(conversionsothersku,0) END) AS SD_AD_conversionsothersku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(conversionsothersku,0) END) AS SP_AD_conversionsothersku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(salessamesku,0) END) AS SD_AD_salessamesku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(salessamesku,0) END) AS SP_AD_salessamesku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(OTHERSKUSALES,0) END) AS SD_AD_OTHERSKUSALES ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(OTHERSKUSALES,0) END) AS SP_AD_OTHERSKUSALES ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(newtobrandsales,0) END) AS SD_AD_newtobrandsales ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(newtobrandsales,0) END) AS SP_AD_newtobrandsales ,sum(case when lower(CAMPAIGN_TYPE) like \'%display%\' then ifnull(newtobrandunits,0) end) as SD_AD_newtobrandunits ,sum(case when lower(CAMPAIGN_TYPE) like \'%product%\' then ifnull(newtobrandunits,0) end) as SP_AD_newtobrandunits ,sum(case when lower(CAMPAIGN_TYPE) like \'%display%\' then ifnull(newtobrandorders,0) end) as SD_AD_newtobrandorders ,sum(case when lower(CAMPAIGN_TYPE) like \'%product%\' then ifnull(newtobrandorders,0) end) as SP_AD_newtobrandorders from GLADFUL_DB.MAPLEMONK.GLADFUL_amazonads_marketing where lower(campaign_type) like any (\'%products%\', \'%display%\') group by DATE ,ASIN ), SBSBVCampaigns as ( select A.Date ,A.ASIN ,C.sessions ,div0(C.sessions,sum(C.sessions) over (partition by A.Date, A.Campaignname)) Session_Share ,A.campaignname ,A.CAMPAIGN_TYPE ,A.IMPRESSIONS ,case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(impressions,0) end SBV_IMPRESSIONS ,case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(impressions,0) end SB_IMPRESSIONS ,SBV_IMPRESSIONS SBV_Impressions_Normalized ,SB_IMPRESSIONS SB_Impressions_Normalized ,(sum(ifnull(SBV_IMPRESSIONS,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SBV_IMPRESSIONS_SESSION_SHARE ,(sum(ifnull(SB_IMPRESSIONS,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SB_IMPRESSIONS_SESSION_SHARE , A.Clicks , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(clicks,0) end SBV_CLICKS , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(clicks,0) end SB_CLICKS , SBV_CLICKS SBV_Clicks_Normalized , SB_CLICKS SB_Clicks_Normalized ,(sum(ifnull(SBV_CLICKS,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SBV_CLICKS_SESSION_SHARE ,(sum(ifnull(SB_CLICKS,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SB_CLICKS_SESSION_SHARE , A.Ad_CONVERSIONS , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(ad_conversions,0) end SBV_AD_CONVERSIONS , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(ad_conversions,0) end SB_AD_CONVERSIONS , SBV_AD_CONVERSIONS SBV_Ad_Conversions_Normalized , SB_AD_CONVERSIONS SB_Ad_Conversions_Normalized ,(sum(ifnull(SBV_AD_CONVERSIONS,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SBV_AD_CONVERSIONS_SESSION_SHARE ,(sum(ifnull(SB_AD_CONVERSIONS,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SB_AD_CONVERSIONS_SESSION_SHARE , A.SPEND , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(spend,0) end SBV_SPEND , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(spend,0) end SB_SPEND , SBV_SPEND SBV_Spend_Normalized , SB_SPEND SB_Spend_Normalized ,(sum(ifnull(SBV_SPEND,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SBV_SPEND_SESSION_SHARE ,(sum(ifnull(SB_SPEND,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SB_SPEND_SESSION_SHARE , A.AD_Sales , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(ad_sales,0) end SBV_AD_SALES , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(ad_sales,0) end SB_AD_SALES , SBV_AD_SALES SBV_Ad_Sales_Normalized , SB_AD_SALES SB_Ad_Sales_Normalized ,(sum(ifnull(SBV_AD_SALES,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SBV_AD_SALES_SESSION_SHARE ,(sum(ifnull(SB_AD_SALES,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SB_AD_SALES_SESSION_SHARE , A.Ad_CONVERSIONSSAMESKU , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(ad_conversionssamesku,0) end SBV_AD_CONVERSIONS_SAME_SKU , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(ad_conversionssamesku,0) end SB_AD_CONVERSIONS_SAME_SKU , SBV_AD_CONVERSIONS_SAME_SKU SBV_Ad_Conversions_Same_SKU_Normalized , SB_AD_CONVERSIONS_SAME_SKU SB_Ad_Conversions_Same_SKU_Normalized ,(sum(ifnull(SBV_AD_CONVERSIONS_SAME_SKU,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SBV_AD_CONVERSIONS_SAME_SKU_SESSION_SHARE ,(sum(ifnull(SB_AD_CONVERSIONS_SAME_SKU,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SB_AD_CONVERSIONS_SAME_SKU_SESSION_SHARE , A.AD_CONVERSIONOTHERSKU , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(AD_conversionothersku,0) end SBV_CONVERSIONOTHERSKU , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(AD_conversionothersku,0) end SB_CONVERSIONOTHERSKU , SBV_CONVERSIONOTHERSKU SBV_ConversionOtherSKU_Normalized , SB_CONVERSIONOTHERSKU SB_ConversionOtherSKU_Normalized ,(sum(ifnull(SBV_CONVERSIONOTHERSKU,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SBV_CONVERSIONOTHERSKU_SESSION_SHARE ,(sum(ifnull(SB_CONVERSIONOTHERSKU,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SB_CONVERSIONOTHERSKU_SESSION_SHARE , A.AD_SalesSAMESKU , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(Ad_salessamesku,0) end SBV_SALES_SAMESKU , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(AD_salessamesku,0) end SB_SALES_SAMESKU , SBV_SALES_SAMESKU SBV_Sales_SameSKU_Normalized , SB_SALES_SAMESKU SB_Sales_SameSKU_Normalized ,(sum(ifnull(SBV_SALES_SAMESKU,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SBV_SALES_SAMESKU_SESSION_SHARE ,(sum(ifnull(SB_SALES_SAMESKU,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SB_SALES_SAMESKU_SESSION_SHARE , A.AD_SalesOTHERSKU , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(AD_salesothersku,0) end SBV_SALES_OTHERSKU , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(AD_salesothersku,0) end SB_SALES_OTHERSKU , SBV_SALES_OTHERSKU SBV_Sales_OtherSKU_Normalized , SB_SALES_OTHERSKU SB_Sales_OtherSKU_Normalized ,(sum(ifnull(SBV_SALES_OTHERSKU,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SBV_SALES_OTHERSKU_SESSION_SHARE ,(sum(ifnull(SB_SALES_OTHERSKU,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SB_SALES_OTHERSKU_SESSION_SHARE , A.AD_NEWTOBRAND_ORDERS , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(ad_newtobrand_orders,0) end SBV_AD_NEWTOBRAND_ORDERS , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(ad_newtobrand_orders,0) end SB_AD_NEWTOBRAND_ORDERS , SBV_AD_NEWTOBRAND_ORDERS SBV_AD_NEWTOBRAND_ORDERS_Normalized , SB_AD_NEWTOBRAND_ORDERS SB_AD_NEWTOBRAND_ORDERS_Normalized ,(sum(ifnull(SBV_AD_NEWTOBRAND_ORDERS,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SBV_AD_NEWTOBRAND_ORDERS_SESSION_SHARE ,(sum(ifnull(SB_AD_NEWTOBRAND_ORDERS,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SB_AD_NEWTOBRAND_ORDERS_SESSION_SHARE , A.AD_NEWTOBRAND_SALES , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(ad_newtobrand_sales,0) end SBV_AD_NEWTOBRAND_SALES , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(ad_newtobrand_sales,0) end SB_AD_NEWTOBRAND_SALES , SBV_AD_NEWTOBRAND_SALES SBV_AD_NEWTOBRAND_SALES_Normalized , SB_AD_NEWTOBRAND_SALES SB_AD_NEWTOBRAND_SALES_Normalized ,(sum(ifnull(SBV_AD_NEWTOBRAND_SALES,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SBV_AD_NEWTOBRAND_SALES_SESSION_SHARE ,(sum(ifnull(SB_AD_NEWTOBRAND_SALES,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SB_AD_NEWTOBRAND_SALES_SESSION_SHARE , A.AD_NEWTOBRAND_UNITS , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(ad_newtobrand_units,0) end SBV_AD_NEWTOBRAND_UNITS , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(ad_newtobrand_units,0) end SB_AD_NEWTOBRAND_UNITS , SBV_AD_NEWTOBRAND_UNITS SBV_AD_NEWTOBRAND_UNITS_Normalized , SB_AD_NEWTOBRAND_UNITS SB_AD_NEWTOBRAND_UNITS_Normalized ,(sum(ifnull(SBV_AD_NEWTOBRAND_UNITS,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SBV_AD_NEWTOBRAND_UNITS_SESSION_SHARE ,(sum(ifnull(SB_AD_NEWTOBRAND_UNITS,0)) over (partition by A.Date, A.Campaignname))*Session_Share as SB_AD_NEWTOBRAND_UNITS_SESSION_SHARE from (select A.DATE ,A.CAMPAIGN_TYPE ,A.campaignname ,B.ASIN ,div0(sum(ifnull(impressions,0)),count(1) over (partition by lower(A.campaignname), A.DATE order by 1)) IMPRESSIONS ,div0(sum(ifnull(clicks,0)),count(1) over (partition by lower(A.campaignname), A.DATE order by 1)) CLICKS ,div0(sum(ifnull(conversions,0)),count(1) over (partition by lower(A.campaignname), A.DATE order by 1)) Ad_CONVERSIONS ,div0(sum(ifnull(SPEND,0)),count(1) over (partition by lower(A.campaignname), A.DATE order by 1)) SPEND ,div0(sum(ifnull(adsales,0)),count(1) over (partition by lower(A.campaignname), A.DATE order by 1)) AD_Sales ,div0(sum(ifnull(conversionssamesku,0)),count(1) over (partition by lower(A.campaignname), A.DATE order by 1)) Ad_CONVERSIONSSAMESKU ,div0(sum(ifnull(conversionsothersku,0)),count(1) over (partition by lower(A.campaignname), A.DATE order by 1)) AD_CONVERSIONOTHERSKU ,div0(sum(ifnull(salessamesku,0)),count(1) over (partition by lower(A.campaignname), A.DATE order by 1)) AD_SalesSAMESKU ,div0(sum(ifnull(OTHERSKUSALES,0)),count(1) over (partition by lower(A.campaignname), A.DATE order by 1)) AD_SalesOTHERSKU ,div0(sum(ifnull(newtobrandorders,0)),count(1) over (partition by lower(A.campaignname), A.DATE order by 1)) AD_NEWTOBRAND_ORDERS ,div0(sum(ifnull(newtobrandsales,0)),count(1) over (partition by lower(A.campaignname), A.DATE order by 1)) AD_NEWTOBRAND_SALES ,div0(sum(ifnull(newtobrandunits,0)),count(1) over (partition by lower(A.campaignname), A.DATE order by 1)) AD_NEWTOBRAND_UNITS from GLADFUL_DB.MAPLEMONK.GLADFUL_amazonads_marketing A left join AdCampaignASINMap B on lower(A.campaignname) = lower(B.campaign) where lower(A.campaign_type) like any (\'%brand%\') group by A.DATE ,A.CAMPAIGN_TYPE ,A.campaignname ,B.ASIN ) A left join sessions C on A.date=C.date and A.ASIN = C.ASIN ), SBSBVASIN_WOALLPRODUCT as ( select Date ,ASIN ,avg(sessions) sessions ,sum(ifnull(SBV_Impressions_Normalized,0)) SBV_Impressions_Same_Share ,sum(ifnull(SB_Impressions_Normalized,0)) SB_Impressions_Same_Share ,sum(ifnull(SBV_Clicks_Normalized,0)) SBV_Clicks_Same_Share ,sum(ifnull(SB_Clicks_Normalized,0)) SB_Clicks_Same_Share ,sum(ifnull(SBV_Ad_Conversions_Normalized,0)) SBV_Ad_Conversions_Same_Share ,sum(ifnull(SB_Ad_Conversions_Normalized,0)) SB_Ad_Conversions_Same_Share ,sum(ifnull(SBV_Spend_Normalized,0)) SBV_Spend_Same_Share ,sum(ifnull(SB_Spend_Normalized,0)) SB_Spend_Same_Share ,sum(ifnull(SBV_Ad_Sales_Normalized,0)) SBV_Ad_Sales_Same_Share ,sum(ifnull(SB_Ad_Sales_Normalized,0)) SB_Ad_Sales_Same_Share ,sum(ifnull(SBV_Ad_Conversions_Same_SKU_Normalized,0)) SBV_Ad_Conversions_Same_SKU_Same_Share ,sum(ifnull(SB_Ad_Conversions_Same_SKU_Normalized,0)) SB_Ad_Conversions_Same_SKU_Same_Share ,sum(ifnull(SBV_ConversionOtherSKU_Normalized,0)) SBV_ConversionOtherSKU_Same_Share ,sum(ifnull(SB_ConversionOtherSKU_Normalized,0)) SB_ConversionOtherSKU_Same_Share ,sum(ifnull(SBV_Sales_SameSKU_Normalized,0)) SBV_Sales_SameSKU_Same_Share ,sum(ifnull(SB_Sales_SameSKU_Normalized,0)) SB_Sales_SameSKU_Same_Share ,sum(ifnull(SBV_Sales_OtherSKU_Normalized,0)) SBV_Sales_OtherSKU_Same_Share ,sum(ifnull(SB_Sales_OtherSKU_Normalized,0)) SB_Sales_OtherSKU_Same_Share ,sum(ifnull(SBV_AD_NEWTOBRAND_ORDERS_Normalized,0)) SBV_AD_NEWTOBRAND_ORDERS_Same_Share ,sum(ifnull(SB_AD_NEWTOBRAND_ORDERS_Normalized,0)) SB_AD_NEWTOBRAND_ORDERS_Same_Share ,sum(ifnull(SBV_AD_NEWTOBRAND_SALES_Normalized,0)) SBV_AD_NEWTOBRAND_SALES_Same_Share ,sum(ifnull(SB_AD_NEWTOBRAND_SALES_Normalized,0)) SB_AD_NEWTOBRAND_SALES_Same_Share ,sum(ifnull(SBV_AD_NEWTOBRAND_UNITS_Normalized,0)) SBV_AD_NEWTOBRAND_UNITS_Same_Share ,sum(ifnull(SB_AD_NEWTOBRAND_UNITS_Normalized,0)) SB_AD_NEWTOBRAND_UNITS_Same_Share ,sum(ifnull(SBV_Impressions_Session_Share,0)) SBV_Impressions_Session_Share ,sum(ifnull(SB_Impressions_Session_Share,0)) SB_Impressions_Session_Share ,sum(ifnull(SBV_Clicks_Session_Share,0)) SBV_Clicks_Session_Share ,sum(ifnull(SB_Clicks_Session_Share,0)) SB_Clicks_Session_Share ,sum(ifnull(SBV_Ad_Conversions_Session_Share,0)) SBV_Ad_Conversions_Session_Share ,sum(ifnull(SB_Ad_Conversions_Session_Share,0)) SB_Ad_Conversions_Session_Share ,sum(ifnull(SBV_Spend_Session_Share,0)) SBV_Spend_Session_Share ,sum(ifnull(SB_Spend_Session_Share,0)) SB_Spend_Session_Share ,sum(ifnull(SBV_Ad_Sales_Session_Share,0)) SBV_Ad_Sales_Session_Share ,sum(ifnull(SB_Ad_Sales_Session_Share,0)) SB_Ad_Sales_Session_Share ,sum(ifnull(SBV_Ad_Conversions_Same_SKU_Session_Share,0)) SBV_Ad_Conversions_Same_SKU_Session_Share ,sum(ifnull(SB_Ad_Conversions_Same_SKU_Session_Share,0)) SB_Ad_Conversions_Same_SKU_Session_Share ,sum(ifnull(SBV_ConversionOtherSKU_Session_Share,0)) SBV_ConversionOtherSKU_Session_Share ,sum(ifnull(SB_ConversionOtherSKU_Session_Share,0)) SB_ConversionOtherSKU_Session_Share ,sum(ifnull(SBV_Sales_SameSKU_Session_Share,0)) SBV_Sales_SameSKU_Session_Share ,sum(ifnull(SB_Sales_SameSKU_Session_Share,0)) SB_Sales_SameSKU_Session_Share ,sum(ifnull(SBV_Sales_OtherSKU_Session_Share,0)) SBV_Sales_OtherSKU_Session_Share ,sum(ifnull(SB_Sales_OtherSKU_Session_Share,0)) SB_Sales_OtherSKU_Session_Share ,sum(ifnull(SBV_AD_NEWTOBRAND_ORDERS_Session_Share,0)) SBV_AD_NEWTOBRAND_ORDERS_Session_Share ,sum(ifnull(SB_AD_NEWTOBRAND_ORDERS_Session_Share,0)) SB_AD_NEWTOBRAND_ORDERS_Session_Share ,sum(ifnull(SBV_AD_NEWTOBRAND_SALES_Session_Share,0)) SBV_AD_NEWTOBRAND_SALES_Session_Share ,sum(ifnull(SB_AD_NEWTOBRAND_SALES_Session_Share,0)) SB_AD_NEWTOBRAND_SALES_Session_Share ,sum(ifnull(SBV_AD_NEWTOBRAND_UNITS_Session_Share,0)) SBV_AD_NEWTOBRAND_UNITS_Session_Share ,sum(ifnull(SB_AD_NEWTOBRAND_UNITS_Session_Share,0)) SB_AD_NEWTOBRAND_UNITS_Session_Share from SBSBVCampaigns where asin is not null group by Date ,ASIN ), SBSBVASIN_ALLPRODUCT as ( select Date ,sum(ifnull(SBV_Impressions_Normalized,0)) SBV_Impressions_ALLPRODUCTS ,sum(ifnull(SB_Impressions_Normalized,0)) SB_Impressions_ALLPRODUCTS ,sum(ifnull(SBV_Clicks_Normalized,0)) SBV_Clicks_ALLPRODUCTS ,sum(ifnull(SB_Clicks_Normalized,0)) SB_Clicks_ALLPRODUCTS ,sum(ifnull(SBV_Ad_Conversions_Normalized,0)) SBV_Ad_Conversions_ALLPRODUCTS ,sum(ifnull(SB_Ad_Conversions_Normalized,0)) SB_Ad_Conversions_ALLPRODUCTS ,sum(ifnull(SBV_Spend_Normalized,0)) SBV_Spend_ALLPRODUCTS ,sum(ifnull(SB_Spend_Normalized,0)) SB_Spend_ALLPRODUCTS ,sum(ifnull(SBV_Ad_Sales_Normalized,0)) SBV_Ad_Sales_ALLPRODUCTS ,sum(ifnull(SB_Ad_Sales_Normalized,0)) SB_Ad_Sales_ALLPRODUCTS ,sum(ifnull(SBV_Ad_Conversions_Same_SKU_Normalized,0)) SBV_Ad_Conversions_Same_SKU_ALLPRODUCTS ,sum(ifnull(SB_Ad_Conversions_Same_SKU_Normalized,0)) SB_Ad_Conversions_Same_SKU_ALLPRODUCTS ,sum(ifnull(SBV_ConversionOtherSKU_Normalized,0)) SBV_ConversionOtherSKU_ALLPRODUCTS ,sum(ifnull(SB_ConversionOtherSKU_Normalized,0)) SB_ConversionOtherSKU_ALLPRODUCTS ,sum(ifnull(SBV_Sales_SameSKU_Normalized,0)) SBV_Sales_SameSKU_ALLPRODUCTS ,sum(ifnull(SB_Sales_SameSKU_Normalized,0)) SB_Sales_SameSKU_ALLPRODUCTS ,sum(ifnull(SBV_Sales_OtherSKU_Normalized,0)) SBV_Sales_OtherSKU_ALLPRODUCTS ,sum(ifnull(SB_Sales_OtherSKU_Normalized,0)) SB_Sales_OtherSKU_ALLPRODUCTS ,sum(ifnull(SBV_AD_NEWTOBRAND_ORDERS_Normalized,0)) SBV_AD_NEWTOBRAND_ORDERS_ALLPRODUCTS ,sum(ifnull(SB_AD_NEWTOBRAND_ORDERS_Normalized,0)) SB_AD_NEWTOBRAND_ORDERS_ALLPRODUCTS ,sum(ifnull(SBV_AD_NEWTOBRAND_SALES_Normalized,0)) SBV_AD_NEWTOBRAND_SALES_ALLPRODUCTS ,sum(ifnull(SB_AD_NEWTOBRAND_SALES_Normalized,0)) SB_AD_NEWTOBRAND_SALES_ALLPRODUCTS ,sum(ifnull(SBV_AD_NEWTOBRAND_UNITS_Normalized,0)) SBV_AD_NEWTOBRAND_UNITS_ALLPRODUCTS ,sum(ifnull(SB_AD_NEWTOBRAND_UNITS_Normalized,0)) SB_AD_NEWTOBRAND_UNITS_ALLPRODUCTS from SBSBVCampaigns where asin is null group by Date ), SBSBVASIN as ( select coalesce(A.Date, B.date) date ,A.ASIN ,A.sessions ,div0(A.sessions, sum(A.sessions) over (partition by coalesce(A.Date, B.date))) Session_Share ,count(1) over (partition by coalesce(A.Date, B.date)) countasins ,(SBV_Impressions_Session_Share + ifnull(B.SBV_Impressions_ALLPRODUCTS,0)*Session_Share) as SBV_Impression_Session_Share ,(SB_Impressions_Session_Share + ifnull(B.SB_Impressions_ALLPRODUCTS,0)*Session_Share) as SB_Impression_Session_Share ,(SBV_Clicks_Session_Share + ifnull(B.SBV_Clicks_ALLPRODUCTS,0)*Session_Share) as SBV_Clicks_Session_Share ,(SB_Clicks_Session_Share + ifnull(B.SB_Clicks_ALLPRODUCTS,0)*Session_Share) as SB_Clicks_Session_Share ,(SBV_Ad_Conversions_Session_Share + ifnull(B.SBV_Ad_Conversions_ALLPRODUCTS,0)*Session_Share) as SBV_Ad_Conversions_Session_Share ,(SB_Ad_Conversions_Session_Share + ifnull(B.SB_Ad_Conversions_ALLPRODUCTS,0)*Session_Share) as SB_Ad_Conversions_Session_Share ,(SBV_Spend_Session_Share + ifnull(B.SBV_Spend_ALLPRODUCTS,0)*Session_Share) as SBV_Spend_Session_Share ,(SB_Spend_Session_Share + ifnull(B.SB_Spend_ALLPRODUCTS,0)*Session_Share) as SB_Spend_Session_Share ,(SBV_Ad_Sales_Session_Share + ifnull(B.SBV_Ad_Sales_ALLPRODUCTS,0)*Session_Share) as SBV_Ad_Sales_Session_Share ,(SB_Ad_Sales_Session_Share + ifnull(B.SB_Ad_Sales_ALLPRODUCTS,0)*Session_Share) as SB_Ad_Sales_Session_Share ,(SBV_Ad_Conversions_Same_SKU_Session_Share + ifnull(B.SBV_Ad_Conversions_Same_SKU_ALLPRODUCTS,0)*Session_Share) as SBV_Ad_Conversions_Same_SKU_Session_Share ,(SB_Ad_Conversions_Same_SKU_Session_Share + ifnull(B.SB_Ad_Conversions_Same_SKU_ALLPRODUCTS,0)*Session_Share) as SB_Ad_Conversions_Same_SKU_Session_Share ,(SBV_ConversionOtherSKU_Session_Share + ifnull(B.SBV_ConversionOtherSKU_ALLPRODUCTS,0)*Session_Share) as SBV_ConversionOtherSKU_Session_Share ,(SB_ConversionOtherSKU_Session_Share + ifnull(B.SB_ConversionOtherSKU_ALLPRODUCTS,0)*Session_Share) as SB_ConversionOtherSKU_Session_Share ,(SBV_Sales_SameSKU_Session_Share + ifnull(B.SBV_Sales_SameSKU_ALLPRODUCTS,0)*Session_Share) as SBV_Sales_SameSKU_Session_Share ,(SB_Sales_SameSKU_Session_Share + ifnull(B.SB_Sales_SameSKU_ALLPRODUCTS,0)*Session_Share) as SB_Sales_SameSKU_Session_Share ,(SBV_Sales_OtherSKU_Session_Share + ifnull(B.SBV_Sales_OtherSKU_ALLPRODUCTS,0)*Session_Share) as SBV_Sales_OtherSKU_Session_Share ,(SB_Sales_OtherSKU_Session_Share + ifnull(B.SB_Sales_OtherSKU_ALLPRODUCTS,0)*Session_Share) as SB_Sales_OtherSKU_Session_Share ,(SBV_AD_NEWTOBRAND_ORDERS_Session_Share + ifnull(B.SBV_AD_NEWTOBRAND_ORDERS_ALLPRODUCTS,0)*Session_Share) as SBV_AD_NEWTOBRAND_ORDERS_Session_Share ,(SB_AD_NEWTOBRAND_ORDERS_Session_Share + ifnull(B.SB_AD_NEWTOBRAND_ORDERS_ALLPRODUCTS,0)*Session_Share) as SB_AD_NEWTOBRAND_ORDERS_Session_Share ,(SBV_AD_NEWTOBRAND_SALES_Session_Share + ifnull(B.SBV_AD_NEWTOBRAND_SALES_ALLPRODUCTS,0)*Session_Share) as SBV_AD_NEWTOBRAND_SALES_Session_Share ,(SB_AD_NEWTOBRAND_SALES_Session_Share + ifnull(B.SB_AD_NEWTOBRAND_SALES_ALLPRODUCTS,0)*Session_Share) as SB_AD_NEWTOBRAND_SALES_Session_Share ,(SBV_AD_NEWTOBRAND_UNITS_Session_Share + ifnull(B.SBV_AD_NEWTOBRAND_UNITS_ALLPRODUCTS,0)*Session_Share) as SBV_AD_NEWTOBRAND_UNITS_Session_Share ,(SB_AD_NEWTOBRAND_UNITS_Session_Share + ifnull(B.SB_AD_NEWTOBRAND_UNITS_ALLPRODUCTS,0)*Session_Share) as SB_AD_NEWTOBRAND_UNITS_Session_Share ,(SBV_Impressions_Same_Share + div0(ifnull(B.SBV_Impressions_ALLPRODUCTS,0),countasins)) as SBV_Impressions_Same_Share ,(SB_Impressions_Same_Share + div0(ifnull(B.SB_Impressions_ALLPRODUCTS,0),countasins)) as SB_Impressions_Same_Share ,(SBV_Clicks_Same_Share + div0(ifnull(B.SBV_Clicks_ALLPRODUCTS,0),countasins)) as SBV_Clicks_Same_Share ,(SB_Clicks_Same_Share + div0(ifnull(B.SB_Clicks_ALLPRODUCTS,0),countasins)) as SB_Clicks_Same_Share ,(SBV_Ad_Conversions_Same_Share + div0(ifnull(B.SBV_Ad_Conversions_ALLPRODUCTS,0),countasins)) as SBV_Ad_Conversions_Same_Share ,(SB_Ad_Conversions_Same_Share + div0(ifnull(B.SB_Ad_Conversions_ALLPRODUCTS,0),countasins)) as SB_Ad_Conversions_Same_Share ,(SBV_Spend_Same_Share + div0(ifnull(B.SBV_Spend_ALLPRODUCTS,0),countasins)) as SBV_Spend_Same_Share ,(SB_Spend_Same_Share + div0(ifnull(B.SB_Spend_ALLPRODUCTS,0),countasins)) as SB_Spend_Same_Share ,(SBV_Ad_Sales_Same_Share + div0(ifnull(B.SBV_Ad_Sales_ALLPRODUCTS,0),countasins)) as SBV_Ad_Sales_Same_Share ,(SB_Ad_Sales_Same_Share + div0(ifnull(B.SB_Ad_Sales_ALLPRODUCTS,0),countasins)) as SB_Ad_Sales_Same_Share ,(SBV_Ad_Conversions_Same_SKU_Same_Share + div0(ifnull(B.SBV_Ad_Conversions_Same_SKU_ALLPRODUCTS,0),countasins)) as SBV_Ad_Conversions_Same_SKU_Same_Share ,(SB_Ad_Conversions_Same_SKU_Same_Share + div0(ifnull(B.SB_Ad_Conversions_Same_SKU_ALLPRODUCTS,0),countasins)) as SB_Ad_Conversions_Same_SKU_Same_Share ,(SBV_ConversionOtherSKU_Same_Share + div0(ifnull(B.SBV_ConversionOtherSKU_ALLPRODUCTS,0),countasins)) as SBV_ConversionOtherSKU_Same_Share ,(SB_ConversionOtherSKU_Same_Share + div0(ifnull(B.SB_ConversionOtherSKU_ALLPRODUCTS,0),countasins)) as SB_ConversionOtherSKU_Same_Share ,(SBV_Sales_SameSKU_Same_Share + div0(ifnull(B.SBV_Sales_SameSKU_ALLPRODUCTS,0),countasins)) as SBV_Sales_SameSKU_Same_Share ,(SB_Sales_SameSKU_Same_Share + div0(ifnull(B.SB_Sales_SameSKU_ALLPRODUCTS,0),countasins)) as SB_Sales_SameSKU_Same_Share ,(SBV_Sales_OtherSKU_Same_Share + div0(ifnull(B.SBV_Sales_OtherSKU_ALLPRODUCTS,0),countasins)) as SBV_Sales_OtherSKU_Same_Share ,(SB_Sales_OtherSKU_Same_Share + div0(ifnull(B.SB_Sales_OtherSKU_ALLPRODUCTS,0),countasins)) as SB_Sales_OtherSKU_Same_Share ,(SBV_AD_NEWTOBRAND_ORDERS_Same_Share + div0(ifnull(B.SBV_AD_NEWTOBRAND_ORDERS_ALLPRODUCTS,0),countasins)) as SBV_AD_NEWTOBRAND_ORDERS_Same_Share ,(SB_AD_NEWTOBRAND_ORDERS_Same_Share + div0(ifnull(B.SB_AD_NEWTOBRAND_ORDERS_ALLPRODUCTS,0),countasins)) as SB_AD_NEWTOBRAND_ORDERS_Same_Share ,(SBV_AD_NEWTOBRAND_SALES_Same_Share + div0(ifnull(B.SBV_AD_NEWTOBRAND_SALES_ALLPRODUCTS,0),countasins)) as SBV_AD_NEWTOBRAND_SALES_Same_Share ,(SB_AD_NEWTOBRAND_SALES_Same_Share + div0(ifnull(B.SB_AD_NEWTOBRAND_SALES_ALLPRODUCTS,0),countasins)) as SB_AD_NEWTOBRAND_SALES_Same_Share ,(SBV_AD_NEWTOBRAND_UNITS_Same_Share + div0(ifnull(B.SBV_AD_NEWTOBRAND_UNITS_ALLPRODUCTS,0),countasins)) as SBV_AD_NEWTOBRAND_UNITS_Same_Share ,(SB_AD_NEWTOBRAND_UNITS_Same_Share + div0(ifnull(B.SB_AD_NEWTOBRAND_UNITS_ALLPRODUCTS,0),countasins)) as SB_AD_NEWTOBRAND_UNITS_Same_Share from SBSBVASIN_WOALLPRODUCT A full outer join SBSBVASIN_ALLPRODUCT B on A.date = B.date ) select coalesce(A.Date, B.Date) Date ,coalesce(A.ASIN, B.ASIN) ASIN ,C.Name Product_Name_Final ,C.Category Category ,C.Sub_category Sub_Category ,SD_IMPRESSIONS ,SP_IMPRESSIONS ,SD_CLICKS ,SP_CLICKS ,SD_AD_conversions ,SP_AD_conversions ,SD_AD_spend ,SP_AD_spend ,SD_adsales ,SP_adsales ,SD_AD_conversionssamesku ,SP_AD_conversionssamesku ,SD_AD_conversionsothersku ,SP_AD_conversionsothersku ,SD_AD_salessamesku ,SP_AD_salessamesku ,SD_AD_OTHERSKUSALES ,SP_AD_OTHERSKUSALES ,SD_AD_newtobrandsales ,SP_AD_newtobrandsales ,SD_AD_newtobrandunits ,SP_AD_newtobrandunits ,SD_AD_newtobrandorders ,SP_AD_newtobrandorders ,SBV_Impression_Session_Share ,SB_Impression_Session_Share ,SBV_Clicks_Session_Share ,SB_Clicks_Session_Share ,SBV_Ad_Conversions_Session_Share ,SB_Ad_Conversions_Session_Share ,SBV_Spend_Session_Share ,SB_Spend_Session_Share ,SBV_Ad_Sales_Session_Share ,SB_Ad_Sales_Session_Share ,SBV_Ad_Conversions_Same_SKU_Session_Share ,SB_Ad_Conversions_Same_SKU_Session_Share ,SBV_ConversionOtherSKU_Session_Share ,SB_ConversionOtherSKU_Session_Share ,SBV_Sales_SameSKU_Session_Share ,SB_Sales_SameSKU_Session_Share ,SBV_Sales_OtherSKU_Session_Share ,SB_Sales_OtherSKU_Session_Share ,SBV_AD_NEWTOBRAND_ORDERS_Session_Share ,SB_AD_NEWTOBRAND_ORDERS_Session_Share ,SBV_AD_NEWTOBRAND_SALES_Session_Share ,SB_AD_NEWTOBRAND_SALES_Session_Share ,SBV_AD_NEWTOBRAND_UNITS_Session_Share ,SB_AD_NEWTOBRAND_UNITS_Session_Share ,SBV_Impressions_Same_Share ,SB_Impressions_Same_Share ,SBV_Clicks_Same_Share ,SB_Clicks_Same_Share ,SBV_Ad_Conversions_Same_Share ,SB_Ad_Conversions_Same_Share ,SBV_Spend_Same_Share ,SB_Spend_Same_Share ,SBV_Ad_Sales_Same_Share ,SB_Ad_Sales_Same_Share ,SBV_Ad_Conversions_Same_SKU_Same_Share ,SB_Ad_Conversions_Same_SKU_Same_Share ,SBV_ConversionOtherSKU_Same_Share ,SB_ConversionOtherSKU_Same_Share ,SBV_Sales_SameSKU_Same_Share ,SB_Sales_SameSKU_Same_Share ,SBV_Sales_OtherSKU_Same_Share ,SB_Sales_OtherSKU_Same_Share ,SBV_AD_NEWTOBRAND_ORDERS_Same_Share ,SB_AD_NEWTOBRAND_ORDERS_Same_Share ,SBV_AD_NEWTOBRAND_SALES_Same_Share ,SB_AD_NEWTOBRAND_SALES_Same_Share ,SBV_AD_NEWTOBRAND_UNITS_Same_Share ,SB_AD_NEWTOBRAND_UNITS_Same_Share ,(ifnull(SBV_Impressions_Same_Share,0) + ifnull(SB_Impressions_Same_Share,0) + ifnull(SD_IMPRESSIONS,0) + ifnull(SP_IMPRESSIONS,0)) as Total_IMPRESSIONS_Same_Share ,(ifnull(SBV_Clicks_Same_Share,0) + ifnull(SB_Clicks_Same_Share,0) + ifnull(SD_CLICKS,0) + ifnull(SP_CLICKS,0)) as Total_CLICKS_Same_Share ,(ifnull(SBV_Ad_Conversions_Same_Share,0) + ifnull(SB_Ad_Conversions_Same_Share,0) + ifnull(SD_AD_conversions,0) + ifnull(SP_AD_conversions,0)) as Total_AD_conversions_Same_Share ,(ifnull(SBV_Spend_Same_Share,0) + ifnull(SB_Spend_Same_Share,0) + ifnull(SD_AD_spend,0) + ifnull(SP_AD_spend,0)) as Total_AD_spend_Same_Share ,(ifnull(SBV_Ad_Sales_Same_Share,0) + ifnull(SB_Ad_Sales_Same_Share,0) + ifnull(SD_adsales,0) + ifnull(SP_adsales,0)) as Total_adsales_Same_Share ,(ifnull(SBV_Ad_Conversions_Same_SKU_Same_Share,0) + ifnull(SB_Ad_Conversions_Same_SKU_Same_Share,0) + ifnull(SD_AD_conversionssamesku,0) + ifnull(SP_AD_conversionssamesku,0)) as Total_AD_conversionssamesku_Same_Share ,(ifnull(SBV_ConversionOtherSKU_Same_Share,0) + ifnull(SB_ConversionOtherSKU_Same_Share,0) + ifnull(SD_AD_conversionsothersku,0) + ifnull(SP_AD_conversionsothersku,0)) as Total_AD_conversionsothersku_Same_Share ,(ifnull(SBV_Sales_SameSKU_Same_Share,0) + ifnull(SB_Sales_SameSKU_Same_Share,0) + ifnull(SD_AD_salessamesku,0) + ifnull(SP_AD_salessamesku,0)) as Total_AD_salessamesku_Same_Share ,(ifnull(SBV_Sales_OtherSKU_Same_Share,0) + ifnull(SB_Sales_OtherSKU_Same_Share,0) + ifnull(SD_AD_OTHERSKUSALES,0) + ifnull(SP_AD_OTHERSKUSALES,0)) as Total_AD_OTHERSKUSALES_Same_Share ,(ifnull(SBV_AD_NEWTOBRAND_ORDERS_Same_Share,0) + ifnull(SB_AD_NEWTOBRAND_ORDERS_Same_Share,0) + ifnull(SD_AD_newtobrandsales,0) + ifnull(SP_AD_newtobrandsales,0)) as Total_AD_newtobrandsales_Same_Share ,(ifnull(SBV_AD_NEWTOBRAND_SALES_Same_Share,0) + ifnull(SB_AD_NEWTOBRAND_SALES_Same_Share,0) + ifnull(SD_AD_newtobrandunits,0) + ifnull(SP_AD_newtobrandunits,0)) as Total_AD_newtobrandunits_Same_Share ,(ifnull(SBV_AD_NEWTOBRAND_UNITS_Same_Share,0) + ifnull(SB_AD_NEWTOBRAND_UNITS_Same_Share,0) + ifnull(SD_AD_newtobrandorders,0) + ifnull(SP_AD_newtobrandorders,0)) as Total_AD_newtobrandorders_Same_Share ,(ifnull(SBV_Impression_Session_Share,0) + ifnull(SB_Impression_Session_Share,0) + ifnull(SD_IMPRESSIONS,0) + ifnull(SP_IMPRESSIONS,0)) as Total_IMPRESSIONS_Session_Share ,(ifnull(SBV_Clicks_Session_Share,0) + ifnull(SB_Clicks_Session_Share,0) + ifnull(SD_CLICKS,0) + ifnull(SP_CLICKS,0)) as Total_CLICKS_Session_Share ,(ifnull(SBV_Ad_Conversions_Session_Share,0) + ifnull(SB_Ad_Conversions_Session_Share,0) + ifnull(SD_AD_conversions,0) + ifnull(SP_AD_conversions,0)) as Total_AD_conversions_Session_Share ,(ifnull(SBV_Spend_Session_Share,0) + ifnull(SB_Spend_Session_Share,0) + ifnull(SD_AD_spend,0) + ifnull(SP_AD_spend,0)) as Total_AD_spend_Session_Share ,(ifnull(SBV_Ad_Sales_Session_Share,0) + ifnull(SB_Ad_Sales_Session_Share,0) + ifnull(SD_adsales,0) + ifnull(SP_adsales,0)) as Total_adsales_Session_Share ,(ifnull(SBV_Ad_Conversions_Same_SKU_Session_Share,0) + ifnull(SB_Ad_Conversions_Same_SKU_Session_Share,0) + ifnull(SD_AD_conversionssamesku,0) + ifnull(SP_AD_conversionssamesku,0)) as Total_AD_conversionssamesku_Session_Share ,(ifnull(SBV_ConversionOtherSKU_Session_Share,0) + ifnull(SB_ConversionOtherSKU_Session_Share,0) + ifnull(SD_AD_conversionsothersku,0) + ifnull(SP_AD_conversionsothersku,0)) as Total_AD_conversionsothersku_Session_Share ,(ifnull(SBV_Sales_SameSKU_Session_Share,0) + ifnull(SB_Sales_SameSKU_Session_Share,0) + ifnull(SD_AD_salessamesku,0) + ifnull(SP_AD_salessamesku,0)) as Total_AD_salessamesku_Session_Share ,(ifnull(SBV_Sales_OtherSKU_Session_Share,0) + ifnull(SB_Sales_OtherSKU_Session_Share,0) + ifnull(SD_AD_OTHERSKUSALES,0) + ifnull(SP_AD_OTHERSKUSALES,0)) as Total_AD_OTHERSKUSALES_Session_Share ,(ifnull(SBV_AD_NEWTOBRAND_ORDERS_Session_Share,0) + ifnull(SB_AD_NEWTOBRAND_ORDERS_Session_Share,0) + ifnull(SD_AD_newtobrandsales,0) + ifnull(SP_AD_newtobrandsales,0)) as Total_AD_newtobrandsales_Session_Share ,(ifnull(SBV_AD_NEWTOBRAND_SALES_Session_Share,0) + ifnull(SB_AD_NEWTOBRAND_SALES_Session_Share,0) + ifnull(SD_AD_newtobrandunits,0) + ifnull(SP_AD_newtobrandunits,0)) as Total_AD_newtobrandunits_Session_Share ,(ifnull(SBV_AD_NEWTOBRAND_UNITS_Session_Share,0) + ifnull(SB_AD_NEWTOBRAND_UNITS_Session_Share,0) + ifnull(SD_AD_newtobrandorders,0) + ifnull(SP_AD_newtobrandorders,0)) as Total_AD_newtobrandorders_Session_Share from SPSDASIN A full outer join SBSBVASIN B on A.Date=B.Date and A.ASIN =B.ASIN left join SKUMASTER C on coalesce(A.ASIN, B.ASIN) = C.MARKETPLACE_Product_ID;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from GLADFUL_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        