{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table perfora_db.maplemonk.perfora_amazonads_consolidated as with SKUMASTER AS ( select * from (select skucode ,product_id ,productname name ,category ,sub_category ,marketplace ,row_number() over (partition by product_id order by product_id) rw from Perfora_DB.MapleMonk.sku_master ) where rw=1 ), Sessions as ( with ASPTraffic as (select queryenddate Date ,parentasin ASIN ,sum(ifnull(trafficbyasin:\"browserPageViews\",0)) Browser_Page_Views ,sum(ifnull(trafficbyasin:\"browserSessions\",0)) Browser_Sessions ,sum(ifnull(trafficbyasin:\"buyBoxPercentage\",0)) BuyBox_Percentage ,sum(ifnull(trafficbyasin:\"mobileAppPageViews\",0)) MobileApp_Page_Views ,sum(ifnull(trafficbyasin:\"mobileAppSessions\",0)) MobileApp_Sessions ,sum(ifnull(trafficbyasin:\"pageViews\",0)) Page_Views ,sum(ifnull(trafficbyasin:\"sessions\",0)) Sessions ,sum(ifnull(sales.value:\"unitsOrdered\"::float,0)) SC_UnitsOrdered ,sum(ifnull(sales.value:\"totalOrderItems\"::float,0)) SC_ItemsOrdered ,sum(ifnull(sales.value:\"orderedProductSales\":\"amount\"::float,0)) SC_Sales from perfora_db.maplemonk.perfora_asp_get_sales_and_traffic_report, lateral flatten (input => salesbyasin) Sales group by queryenddate, parentasin ) select ASPTRAFFIC.* ,SKUMASTER.name Product_Name_Final ,SKUMASTER.category Mapped_Product_Category ,SKUMASTER.sub_category Mapped_Sub_Category ,SKUMASTER.skucode SKU from ASPTRAFFIC left join SKUMASTER on ASPTRAFFIC.ASIN=SKUMASTER.PRODUCT_ID ), AdCampaignASINMap as ( select * from (Select TYPE Campaign_type ,campaign ,category sub_category ,targeting ,remarketing ,\"Branded/Non Brand\" ,row_number() over (partition by lower(campaign) order by 1) rw from perfora_db.maplemonk.perfora_gs_amazon_brand_campaign_mapping ) where rw=1 ), SPSDASIN as ( select DATE ,ASIN ,sum(case when lower(CAMPAIGN_TYPE) like \'%display%\' then ifnull(impressions,0) end) SD_IMPRESSIONS ,sum(case when lower(CAMPAIGN_TYPE) like \'%product%\' then ifnull(impressions,0) end) SP_IMPRESSIONS ,sum(case when lower(CAMPAIGN_TYPE) like \'%display%\' then ifnull(clicks,0) end) SD_CLICKS ,sum(case when lower(CAMPAIGN_TYPE) like \'%product%\' then ifnull(clicks,0) end) SP_CLICKS ,sum(case when lower(CAMPAIGN_TYPE) like \'%display%\' then ifnull(conversions,0) end) SD_AD_conversions ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(conversions,0) END) AS SP_AD_conversions ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(spend,0) END) AS SD_AD_spend ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(spend,0) END) AS SP_AD_spend ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(adsales,0) END) AS SD_adsales ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(adsales,0) END) AS SP_adsales ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(conversionssamesku,0) END) AS SD_AD_conversionssamesku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(conversionssamesku,0) END) AS SP_AD_conversionssamesku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(conversionsothersku,0) END) AS SD_AD_conversionsothersku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(conversionsothersku,0) END) AS SP_AD_conversionsothersku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(salessamesku,0) END) AS SD_AD_salessamesku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(salessamesku,0) END) AS SP_AD_salessamesku ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(OTHERSKUSALES,0) END) AS SD_AD_OTHERSKUSALES ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(OTHERSKUSALES,0) END) AS SP_AD_OTHERSKUSALES ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%display%\' THEN IFNULL(newtobrandsales,0) END) AS SD_AD_newtobrandsales ,SUM(CASE WHEN LOWER(CAMPAIGN_TYPE) LIKE \'%product%\' THEN IFNULL(newtobrandsales,0) END) AS SP_AD_newtobrandsales ,sum(case when lower(CAMPAIGN_TYPE) like \'%display%\' then ifnull(newtobrandunits,0) end) as SD_AD_newtobrandunits ,sum(case when lower(CAMPAIGN_TYPE) like \'%product%\' then ifnull(newtobrandunits,0) end) as SP_AD_newtobrandunits ,sum(case when lower(CAMPAIGN_TYPE) like \'%display%\' then ifnull(newtobrandorders,0) end) as SD_AD_newtobrandorders ,sum(case when lower(CAMPAIGN_TYPE) like \'%product%\' then ifnull(newtobrandorders,0) end) as SP_AD_newtobrandorders from perfora_db.maplemonk.perfora_db_amazonads_marketing where lower(campaign_type) like any (\'%products%\', \'%display%\') group by DATE ,ASIN ), SBSBVCampaigns as ( select A.Date ,B.product_id ASIN ,C.sessions ,div0(C.sessions,sum(C.sessions) over (partition by A.Date, A.Campaignname)) Session_Share ,A.sub_Category ,A.campaignname ,A.CAMPAIGN_TYPE ,A.IMPRESSIONS ,case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(impressions,0) end SBV_IMPRESSIONS ,case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(impressions,0) end SB_IMPRESSIONS ,div0(SBV_IMPRESSIONS,count(1) over (partition by A.Date, A.Campaignname)) SBV_Impressions_Normalized ,div0(SB_IMPRESSIONS,count(1) over (partition by A.Date, A.Campaignname)) SB_Impressions_Normalized ,ifnull(SBV_IMPRESSIONS,0)*Session_Share as SBV_IMPRESSIONS_SESSION_SHARE ,ifnull(SB_IMPRESSIONS,0)*Session_Share as SB_IMPRESSIONS_SESSION_SHARE , A.Clicks , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(clicks,0) end SBV_CLICKS , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(clicks,0) end SB_CLICKS , div0(SBV_CLICKS, count(1) over (partition by A.Date, A.Campaignname)) SBV_Clicks_Normalized , div0(SB_CLICKS, count(1) over (partition by A.Date, A.Campaignname)) SB_Clicks_Normalized ,ifnull(SBV_CLICKS,0)*Session_Share as SBV_CLICKS_SESSION_SHARE ,ifnull(SB_CLICKS,0)*Session_Share as SB_CLICKS_SESSION_SHARE , A.Ad_CONVERSIONS , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(ad_conversions,0) end SBV_AD_CONVERSIONS , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(ad_conversions,0) end SB_AD_CONVERSIONS , div0(SBV_AD_CONVERSIONS, count(1) over (partition by A.Date, A.Campaignname)) SBV_Ad_Conversions_Normalized , div0(SB_AD_CONVERSIONS, count(1) over (partition by A.Date, A.Campaignname)) SB_Ad_Conversions_Normalized ,ifnull(SBV_AD_CONVERSIONS,0)*Session_Share as SBV_AD_CONVERSIONS_SESSION_SHARE ,ifnull(SB_AD_CONVERSIONS,0)*Session_Share as SB_AD_CONVERSIONS_SESSION_SHARE , A.SPEND , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(spend,0) end SBV_SPEND , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(spend,0) end SB_SPEND , div0(SBV_SPEND, count(1) over (partition by A.Date, A.Campaignname)) SBV_Spend_Normalized , div0(SB_SPEND, count(1) over (partition by A.Date, A.Campaignname)) SB_Spend_Normalized ,ifnull(SBV_SPEND,0)*Session_Share as SBV_SPEND_SESSION_SHARE ,ifnull(SB_SPEND,0)*Session_Share as SB_SPEND_SESSION_SHARE , A.AD_Sales , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(ad_sales,0) end SBV_AD_SALES , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(ad_sales,0) end SB_AD_SALES , div0(SBV_AD_SALES, count(1) over (partition by A.Date, A.Campaignname)) SBV_Ad_Sales_Normalized , div0(SB_AD_SALES, count(1) over (partition by A.Date, A.Campaignname)) SB_Ad_Sales_Normalized ,ifnull(SBV_AD_SALES,0)*Session_Share as SBV_AD_SALES_SESSION_SHARE ,ifnull(SB_AD_SALES,0)*Session_Share as SB_AD_SALES_SESSION_SHARE , A.Ad_CONVERSIONSSAMESKU , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(ad_conversionssamesku,0) end SBV_AD_CONVERSIONS_SAME_SKU , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(ad_conversionssamesku,0) end SB_AD_CONVERSIONS_SAME_SKU , div0(SBV_AD_CONVERSIONS_SAME_SKU, count(1) over (partition by A.Date, A.Campaignname)) SBV_Ad_Conversions_Same_SKU_Normalized , div0(SB_AD_CONVERSIONS_SAME_SKU, count(1) over (partition by A.Date, A.Campaignname)) SB_Ad_Conversions_Same_SKU_Normalized ,ifnull(SBV_AD_CONVERSIONS_SAME_SKU,0)*Session_Share as SBV_AD_CONVERSIONS_SAME_SKU_SESSION_SHARE ,ifnull(SB_AD_CONVERSIONS_SAME_SKU,0)*Session_Share as SB_AD_CONVERSIONS_SAME_SKU_SESSION_SHARE , A.AD_CONVERSIONOTHERSKU , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(AD_conversionothersku,0) end SBV_CONVERSIONOTHERSKU , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(AD_conversionothersku,0) end SB_CONVERSIONOTHERSKU , div0(SBV_CONVERSIONOTHERSKU, count(1) over (partition by A.Date, A.Campaignname)) SBV_ConversionOtherSKU_Normalized , div0(SB_CONVERSIONOTHERSKU, count(1) over (partition by A.Date, A.Campaignname)) SB_ConversionOtherSKU_Normalized ,ifnull(SBV_CONVERSIONOTHERSKU,0)*Session_Share as SBV_CONVERSIONOTHERSKU_SESSION_SHARE ,ifnull(SB_CONVERSIONOTHERSKU,0)*Session_Share as SB_CONVERSIONOTHERSKU_SESSION_SHARE , A.AD_SalesSAMESKU , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(Ad_salessamesku,0) end SBV_SALES_SAMESKU , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(AD_salessamesku,0) end SB_SALES_SAMESKU , div0(SBV_SALES_SAMESKU, count(1) over (partition by A.Date, A.Campaignname)) SBV_Sales_SameSKU_Normalized , div0(SB_SALES_SAMESKU, count(1) over (partition by A.Date, A.Campaignname)) SB_Sales_SameSKU_Normalized ,ifnull(SBV_SALES_SAMESKU,0)*Session_Share as SBV_SALES_SAMESKU_SESSION_SHARE ,ifnull(SB_SALES_SAMESKU,0)*Session_Share as SB_SALES_SAMESKU_SESSION_SHARE , A.AD_SalesOTHERSKU , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(AD_salesothersku,0) end SBV_SALES_OTHERSKU , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(AD_salesothersku,0) end SB_SALES_OTHERSKU , div0(SBV_SALES_OTHERSKU, count(1) over (partition by A.Date, A.Campaignname)) SBV_Sales_OtherSKU_Normalized , div0(SB_SALES_OTHERSKU, count(1) over (partition by A.Date, A.Campaignname)) SB_Sales_OtherSKU_Normalized ,ifnull(SBV_SALES_OTHERSKU,0)*Session_Share as SBV_SALES_OTHERSKU_SESSION_SHARE ,ifnull(SB_SALES_OTHERSKU,0)*Session_Share as SB_SALES_OTHERSKU_SESSION_SHARE , A.AD_NEWTOBRAND_ORDERS , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(ad_newtobrand_orders,0) end SBV_AD_NEWTOBRAND_ORDERS , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(ad_newtobrand_orders,0) end SB_AD_NEWTOBRAND_ORDERS , div0(SBV_AD_NEWTOBRAND_ORDERS, count(1) over (partition by A.Date, A.Campaignname)) SBV_AD_NEWTOBRAND_ORDERS_Normalized , div0(SB_AD_NEWTOBRAND_ORDERS, count(1) over (partition by A.Date, A.Campaignname)) SB_AD_NEWTOBRAND_ORDERS_Normalized ,ifnull(SBV_AD_NEWTOBRAND_ORDERS,0)*Session_Share as SBV_AD_NEWTOBRAND_ORDERS_SESSION_SHARE ,ifnull(SB_AD_NEWTOBRAND_ORDERS,0)*Session_Share as SB_AD_NEWTOBRAND_ORDERS_SESSION_SHARE , A.AD_NEWTOBRAND_SALES , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(ad_newtobrand_sales,0) end SBV_AD_NEWTOBRAND_SALES , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(ad_newtobrand_sales,0) end SB_AD_NEWTOBRAND_SALES , div0(SBV_AD_NEWTOBRAND_SALES, count(1) over (partition by A.Date, A.Campaignname)) SBV_AD_NEWTOBRAND_SALES_Normalized , div0(SB_AD_NEWTOBRAND_SALES, count(1) over (partition by A.Date, A.Campaignname)) SB_AD_NEWTOBRAND_SALES_Normalized ,ifnull(SBV_AD_NEWTOBRAND_SALES,0)*Session_Share as SBV_AD_NEWTOBRAND_SALES_SESSION_SHARE ,ifnull(SB_AD_NEWTOBRAND_SALES,0)*Session_Share as SB_AD_NEWTOBRAND_SALES_SESSION_SHARE , A.AD_NEWTOBRAND_UNITS , case when lower(CAMPAIGN_TYPE) = \'sponsored brands video\' then ifnull(ad_newtobrand_units,0) end SBV_AD_NEWTOBRAND_UNITS , case when lower(CAMPAIGN_TYPE) = \'sponsored brands\' then ifnull(ad_newtobrand_units,0) end SB_AD_NEWTOBRAND_UNITS , div0(SBV_AD_NEWTOBRAND_UNITS, count(1) over (partition by A.Date, A.Campaignname)) SBV_AD_NEWTOBRAND_UNITS_Normalized , div0(SB_AD_NEWTOBRAND_UNITS, count(1) over (partition by A.Date, A.Campaignname)) SB_AD_NEWTOBRAND_UNITS_Normalized ,ifnull(SBV_AD_NEWTOBRAND_UNITS,0)*Session_Share as SBV_AD_NEWTOBRAND_UNITS_SESSION_SHARE ,ifnull(SB_AD_NEWTOBRAND_UNITS,0)*Session_Share as SB_AD_NEWTOBRAND_UNITS_SESSION_SHARE from (select A.DATE ,A.CAMPAIGN_TYPE ,A.campaignname ,B.sub_category ,sum(ifnull(impressions,0)) IMPRESSIONS ,sum(ifnull(clicks,0)) CLICKS ,sum(ifnull(conversions,0)) Ad_CONVERSIONS ,sum(ifnull(spend,0)) SPEND ,sum(ifnull(adsales,0)) AD_Sales ,sum(ifnull(conversionssamesku,0)) Ad_CONVERSIONSSAMESKU ,sum(ifnull(conversionsothersku,0)) AD_CONVERSIONOTHERSKU ,sum(ifnull(salessamesku,0)) AD_SalesSAMESKU ,sum(ifnull(OTHERSKUSALES,0)) AD_SalesOTHERSKU ,sum(ifnull(newtobrandorders,0)) AD_NEWTOBRAND_ORDERS ,sum(ifnull(newtobrandsales,0)) AD_NEWTOBRAND_SALES ,sum(ifnull(newtobrandunits,0)) AD_NEWTOBRAND_UNITS from perfora_db.maplemonk.perfora_db_amazonads_marketing A left join AdCampaignASINMap B on lower(A.campaignname) = lower(B.campaign) where lower(A.campaign_type) like any (\'%brand%\') group by A.DATE ,A.CAMPAIGN_TYPE ,A.campaignname ,B.sub_category ) A left join (Select * from SKUMASTER where lower(marketplace) like \'%amazon%\') B on lower(A.sub_category) =lower(B.sub_category) left join sessions C on A.date=C.date and B.product_id = C.ASIN ), SBSBVASIN_WOALLPRODUCT as ( select Date ,ASIN ,avg(sessions) sessions ,sum(ifnull(SBV_Impressions_Normalized,0)) SBV_Impressions_Same_Share ,sum(ifnull(SB_Impressions_Normalized,0)) SB_Impressions_Same_Share ,sum(ifnull(SBV_Clicks_Normalized,0)) SBV_Clicks_Same_Share ,sum(ifnull(SB_Clicks_Normalized,0)) SB_Clicks_Same_Share ,sum(ifnull(SBV_Ad_Conversions_Normalized,0)) SBV_Ad_Conversions_Same_Share ,sum(ifnull(SB_Ad_Conversions_Normalized,0)) SB_Ad_Conversions_Same_Share ,sum(ifnull(SBV_Spend_Normalized,0)) SBV_Spend_Same_Share ,sum(ifnull(SB_Spend_Normalized,0)) SB_Spend_Same_Share ,sum(ifnull(SBV_Ad_Sales_Normalized,0)) SBV_Ad_Sales_Same_Share ,sum(ifnull(SB_Ad_Sales_Normalized,0)) SB_Ad_Sales_Same_Share ,sum(ifnull(SBV_Ad_Conversions_Same_SKU_Normalized,0)) SBV_Ad_Conversions_Same_SKU_Same_Share ,sum(ifnull(SB_Ad_Conversions_Same_SKU_Normalized,0)) SB_Ad_Conversions_Same_SKU_Same_Share ,sum(ifnull(SBV_ConversionOtherSKU_Normalized,0)) SBV_ConversionOtherSKU_Same_Share ,sum(ifnull(SB_ConversionOtherSKU_Normalized,0)) SB_ConversionOtherSKU_Same_Share ,sum(ifnull(SBV_Sales_SameSKU_Normalized,0)) SBV_Sales_SameSKU_Same_Share ,sum(ifnull(SB_Sales_SameSKU_Normalized,0)) SB_Sales_SameSKU_Same_Share ,sum(ifnull(SBV_Sales_OtherSKU_Normalized,0)) SBV_Sales_OtherSKU_Same_Share ,sum(ifnull(SB_Sales_OtherSKU_Normalized,0)) SB_Sales_OtherSKU_Same_Share ,sum(ifnull(SBV_AD_NEWTOBRAND_ORDERS_Normalized,0)) SBV_AD_NEWTOBRAND_ORDERS_Same_Share ,sum(ifnull(SB_AD_NEWTOBRAND_ORDERS_Normalized,0)) SB_AD_NEWTOBRAND_ORDERS_Same_Share ,sum(ifnull(SBV_AD_NEWTOBRAND_SALES_Normalized,0)) SBV_AD_NEWTOBRAND_SALES_Same_Share ,sum(ifnull(SB_AD_NEWTOBRAND_SALES_Normalized,0)) SB_AD_NEWTOBRAND_SALES_Same_Share ,sum(ifnull(SBV_AD_NEWTOBRAND_UNITS_Normalized,0)) SBV_AD_NEWTOBRAND_UNITS_Same_Share ,sum(ifnull(SB_AD_NEWTOBRAND_UNITS_Normalized,0)) SB_AD_NEWTOBRAND_UNITS_Same_Share ,sum(ifnull(SBV_Impressions_Session_Share,0)) SBV_Impressions_Session_Share ,sum(ifnull(SB_Impressions_Session_Share,0)) SB_Impressions_Session_Share ,sum(ifnull(SBV_Clicks_Session_Share,0)) SBV_Clicks_Session_Share ,sum(ifnull(SB_Clicks_Session_Share,0)) SB_Clicks_Session_Share ,sum(ifnull(SBV_Ad_Conversions_Session_Share,0)) SBV_Ad_Conversions_Session_Share ,sum(ifnull(SB_Ad_Conversions_Session_Share,0)) SB_Ad_Conversions_Session_Share ,sum(ifnull(SBV_Spend_Session_Share,0)) SBV_Spend_Session_Share ,sum(ifnull(SB_Spend_Session_Share,0)) SB_Spend_Session_Share ,sum(ifnull(SBV_Ad_Sales_Session_Share,0)) SBV_Ad_Sales_Session_Share ,sum(ifnull(SB_Ad_Sales_Session_Share,0)) SB_Ad_Sales_Session_Share ,sum(ifnull(SBV_Ad_Conversions_Same_SKU_Session_Share,0)) SBV_Ad_Conversions_Same_SKU_Session_Share ,sum(ifnull(SB_Ad_Conversions_Same_SKU_Session_Share,0)) SB_Ad_Conversions_Same_SKU_Session_Share ,sum(ifnull(SBV_ConversionOtherSKU_Session_Share,0)) SBV_ConversionOtherSKU_Session_Share ,sum(ifnull(SB_ConversionOtherSKU_Session_Share,0)) SB_ConversionOtherSKU_Session_Share ,sum(ifnull(SBV_Sales_SameSKU_Session_Share,0)) SBV_Sales_SameSKU_Session_Share ,sum(ifnull(SB_Sales_SameSKU_Session_Share,0)) SB_Sales_SameSKU_Session_Share ,sum(ifnull(SBV_Sales_OtherSKU_Session_Share,0)) SBV_Sales_OtherSKU_Session_Share ,sum(ifnull(SB_Sales_OtherSKU_Session_Share,0)) SB_Sales_OtherSKU_Session_Share ,sum(ifnull(SBV_AD_NEWTOBRAND_ORDERS_Session_Share,0)) SBV_AD_NEWTOBRAND_ORDERS_Session_Share ,sum(ifnull(SB_AD_NEWTOBRAND_ORDERS_Session_Share,0)) SB_AD_NEWTOBRAND_ORDERS_Session_Share ,sum(ifnull(SBV_AD_NEWTOBRAND_SALES_Session_Share,0)) SBV_AD_NEWTOBRAND_SALES_Session_Share ,sum(ifnull(SB_AD_NEWTOBRAND_SALES_Session_Share,0)) SB_AD_NEWTOBRAND_SALES_Session_Share ,sum(ifnull(SBV_AD_NEWTOBRAND_UNITS_Session_Share,0)) SBV_AD_NEWTOBRAND_UNITS_Session_Share ,sum(ifnull(SB_AD_NEWTOBRAND_UNITS_Session_Share,0)) SB_AD_NEWTOBRAND_UNITS_Session_Share from SBSBVCampaigns where asin is not null group by Date ,ASIN ), SBSBVASIN_ALLPRODUCT as ( select Date ,sum(ifnull(SBV_Impressions_Normalized,0)) SBV_Impressions_ALLPRODUCTS ,sum(ifnull(SB_Impressions_Normalized,0)) SB_Impressions_ALLPRODUCTS ,sum(ifnull(SBV_Clicks_Normalized,0)) SBV_Clicks_ALLPRODUCTS ,sum(ifnull(SB_Clicks_Normalized,0)) SB_Clicks_ALLPRODUCTS ,sum(ifnull(SBV_Ad_Conversions_Normalized,0)) SBV_Ad_Conversions_ALLPRODUCTS ,sum(ifnull(SB_Ad_Conversions_Normalized,0)) SB_Ad_Conversions_ALLPRODUCTS ,sum(ifnull(SBV_Spend_Normalized,0)) SBV_Spend_ALLPRODUCTS ,sum(ifnull(SB_Spend_Normalized,0)) SB_Spend_ALLPRODUCTS ,sum(ifnull(SBV_Ad_Sales_Normalized,0)) SBV_Ad_Sales_ALLPRODUCTS ,sum(ifnull(SB_Ad_Sales_Normalized,0)) SB_Ad_Sales_ALLPRODUCTS ,sum(ifnull(SBV_Ad_Conversions_Same_SKU_Normalized,0)) SBV_Ad_Conversions_Same_SKU_ALLPRODUCTS ,sum(ifnull(SB_Ad_Conversions_Same_SKU_Normalized,0)) SB_Ad_Conversions_Same_SKU_ALLPRODUCTS ,sum(ifnull(SBV_ConversionOtherSKU_Normalized,0)) SBV_ConversionOtherSKU_ALLPRODUCTS ,sum(ifnull(SB_ConversionOtherSKU_Normalized,0)) SB_ConversionOtherSKU_ALLPRODUCTS ,sum(ifnull(SBV_Sales_SameSKU_Normalized,0)) SBV_Sales_SameSKU_ALLPRODUCTS ,sum(ifnull(SB_Sales_SameSKU_Normalized,0)) SB_Sales_SameSKU_ALLPRODUCTS ,sum(ifnull(SBV_Sales_OtherSKU_Normalized,0)) SBV_Sales_OtherSKU_ALLPRODUCTS ,sum(ifnull(SB_Sales_OtherSKU_Normalized,0)) SB_Sales_OtherSKU_ALLPRODUCTS ,sum(ifnull(SBV_AD_NEWTOBRAND_ORDERS_Normalized,0)) SBV_AD_NEWTOBRAND_ORDERS_ALLPRODUCTS ,sum(ifnull(SB_AD_NEWTOBRAND_ORDERS_Normalized,0)) SB_AD_NEWTOBRAND_ORDERS_ALLPRODUCTS ,sum(ifnull(SBV_AD_NEWTOBRAND_SALES_Normalized,0)) SBV_AD_NEWTOBRAND_SALES_ALLPRODUCTS ,sum(ifnull(SB_AD_NEWTOBRAND_SALES_Normalized,0)) SB_AD_NEWTOBRAND_SALES_ALLPRODUCTS ,sum(ifnull(SBV_AD_NEWTOBRAND_UNITS_Normalized,0)) SBV_AD_NEWTOBRAND_UNITS_ALLPRODUCTS ,sum(ifnull(SB_AD_NEWTOBRAND_UNITS_Normalized,0)) SB_AD_NEWTOBRAND_UNITS_ALLPRODUCTS from SBSBVCampaigns where asin is null group by Date ), SBSBVASIN as ( select coalesce(A.Date, B.date) date ,A.ASIN ,A.sessions ,div0(A.sessions, sum(A.sessions) over (partition by coalesce(A.Date, B.date))) Session_Share ,count(1) over (partition by coalesce(A.Date, B.date)) countasins ,(SBV_Impressions_Session_Share + ifnull(B.SBV_Impressions_ALLPRODUCTS,0)*Session_Share) as SBV_Impression_Session_Share ,(SB_Impressions_Session_Share + ifnull(B.SB_Impressions_ALLPRODUCTS,0)*Session_Share) as SB_Impression_Session_Share ,(SBV_Clicks_Session_Share + ifnull(B.SBV_Clicks_ALLPRODUCTS,0)*Session_Share) as SBV_Clicks_Session_Share ,(SB_Clicks_Session_Share + ifnull(B.SB_Clicks_ALLPRODUCTS,0)*Session_Share) as SB_Clicks_Session_Share ,(SBV_Ad_Conversions_Session_Share + ifnull(B.SBV_Ad_Conversions_ALLPRODUCTS,0)*Session_Share) as SBV_Ad_Conversions_Session_Share ,(SB_Ad_Conversions_Session_Share + ifnull(B.SB_Ad_Conversions_ALLPRODUCTS,0)*Session_Share) as SB_Ad_Conversions_Session_Share ,(SBV_Spend_Session_Share + ifnull(B.SBV_Spend_ALLPRODUCTS,0)*Session_Share) as SBV_Spend_Session_Share ,(SB_Spend_Session_Share + ifnull(B.SB_Spend_ALLPRODUCTS,0)*Session_Share) as SB_Spend_Session_Share ,(SBV_Ad_Sales_Session_Share + ifnull(B.SBV_Ad_Sales_ALLPRODUCTS,0)*Session_Share) as SBV_Ad_Sales_Session_Share ,(SB_Ad_Sales_Session_Share + ifnull(B.SB_Ad_Sales_ALLPRODUCTS,0)*Session_Share) as SB_Ad_Sales_Session_Share ,(SBV_Ad_Conversions_Same_SKU_Session_Share + ifnull(B.SBV_Ad_Conversions_Same_SKU_ALLPRODUCTS,0)*Session_Share) as SBV_Ad_Conversions_Same_SKU_Session_Share ,(SB_Ad_Conversions_Same_SKU_Session_Share + ifnull(B.SB_Ad_Conversions_Same_SKU_ALLPRODUCTS,0)*Session_Share) as SB_Ad_Conversions_Same_SKU_Session_Share ,(SBV_ConversionOtherSKU_Session_Share + ifnull(B.SBV_ConversionOtherSKU_ALLPRODUCTS,0)*Session_Share) as SBV_ConversionOtherSKU_Session_Share ,(SB_ConversionOtherSKU_Session_Share + ifnull(B.SB_ConversionOtherSKU_ALLPRODUCTS,0)*Session_Share) as SB_ConversionOtherSKU_Session_Share ,(SBV_Sales_SameSKU_Session_Share + ifnull(B.SBV_Sales_SameSKU_ALLPRODUCTS,0)*Session_Share) as SBV_Sales_SameSKU_Session_Share ,(SB_Sales_SameSKU_Session_Share + ifnull(B.SB_Sales_SameSKU_ALLPRODUCTS,0)*Session_Share) as SB_Sales_SameSKU_Session_Share ,(SBV_Sales_OtherSKU_Session_Share + ifnull(B.SBV_Sales_OtherSKU_ALLPRODUCTS,0)*Session_Share) as SBV_Sales_OtherSKU_Session_Share ,(SB_Sales_OtherSKU_Session_Share + ifnull(B.SB_Sales_OtherSKU_ALLPRODUCTS,0)*Session_Share) as SB_Sales_OtherSKU_Session_Share ,(SBV_AD_NEWTOBRAND_ORDERS_Session_Share + ifnull(B.SBV_AD_NEWTOBRAND_ORDERS_ALLPRODUCTS,0)*Session_Share) as SBV_AD_NEWTOBRAND_ORDERS_Session_Share ,(SB_AD_NEWTOBRAND_ORDERS_Session_Share + ifnull(B.SB_AD_NEWTOBRAND_ORDERS_ALLPRODUCTS,0)*Session_Share) as SB_AD_NEWTOBRAND_ORDERS_Session_Share ,(SBV_AD_NEWTOBRAND_SALES_Session_Share + ifnull(B.SBV_AD_NEWTOBRAND_SALES_ALLPRODUCTS,0)*Session_Share) as SBV_AD_NEWTOBRAND_SALES_Session_Share ,(SB_AD_NEWTOBRAND_SALES_Session_Share + ifnull(B.SB_AD_NEWTOBRAND_SALES_ALLPRODUCTS,0)*Session_Share) as SB_AD_NEWTOBRAND_SALES_Session_Share ,(SBV_AD_NEWTOBRAND_UNITS_Session_Share + ifnull(B.SBV_AD_NEWTOBRAND_UNITS_ALLPRODUCTS,0)*Session_Share) as SBV_AD_NEWTOBRAND_UNITS_Session_Share ,(SB_AD_NEWTOBRAND_UNITS_Session_Share + ifnull(B.SB_AD_NEWTOBRAND_UNITS_ALLPRODUCTS,0)*Session_Share) as SB_AD_NEWTOBRAND_UNITS_Session_Share ,(SBV_Impressions_Same_Share + div0(ifnull(B.SBV_Impressions_ALLPRODUCTS,0),countasins)) as SBV_Impressions_Same_Share ,(SB_Impressions_Same_Share + div0(ifnull(B.SB_Impressions_ALLPRODUCTS,0),countasins)) as SB_Impressions_Same_Share ,(SBV_Clicks_Same_Share + div0(ifnull(B.SBV_Clicks_ALLPRODUCTS,0),countasins)) as SBV_Clicks_Same_Share ,(SB_Clicks_Same_Share + div0(ifnull(B.SB_Clicks_ALLPRODUCTS,0),countasins)) as SB_Clicks_Same_Share ,(SBV_Ad_Conversions_Same_Share + div0(ifnull(B.SBV_Ad_Conversions_ALLPRODUCTS,0),countasins)) as SBV_Ad_Conversions_Same_Share ,(SB_Ad_Conversions_Same_Share + div0(ifnull(B.SB_Ad_Conversions_ALLPRODUCTS,0),countasins)) as SB_Ad_Conversions_Same_Share ,(SBV_Spend_Same_Share + div0(ifnull(B.SBV_Spend_ALLPRODUCTS,0),countasins)) as SBV_Spend_Same_Share ,(SB_Spend_Same_Share + div0(ifnull(B.SB_Spend_ALLPRODUCTS,0),countasins)) as SB_Spend_Same_Share ,(SBV_Ad_Sales_Same_Share + div0(ifnull(B.SBV_Ad_Sales_ALLPRODUCTS,0),countasins)) as SBV_Ad_Sales_Same_Share ,(SB_Ad_Sales_Same_Share + div0(ifnull(B.SB_Ad_Sales_ALLPRODUCTS,0),countasins)) as SB_Ad_Sales_Same_Share ,(SBV_Ad_Conversions_Same_SKU_Same_Share + div0(ifnull(B.SBV_Ad_Conversions_Same_SKU_ALLPRODUCTS,0),countasins)) as SBV_Ad_Conversions_Same_SKU_Same_Share ,(SB_Ad_Conversions_Same_SKU_Same_Share + div0(ifnull(B.SB_Ad_Conversions_Same_SKU_ALLPRODUCTS,0),countasins)) as SB_Ad_Conversions_Same_SKU_Same_Share ,(SBV_ConversionOtherSKU_Same_Share + div0(ifnull(B.SBV_ConversionOtherSKU_ALLPRODUCTS,0),countasins)) as SBV_ConversionOtherSKU_Same_Share ,(SB_ConversionOtherSKU_Same_Share + div0(ifnull(B.SB_ConversionOtherSKU_ALLPRODUCTS,0),countasins)) as SB_ConversionOtherSKU_Same_Share ,(SBV_Sales_SameSKU_Same_Share + div0(ifnull(B.SBV_Sales_SameSKU_ALLPRODUCTS,0),countasins)) as SBV_Sales_SameSKU_Same_Share ,(SB_Sales_SameSKU_Same_Share + div0(ifnull(B.SB_Sales_SameSKU_ALLPRODUCTS,0),countasins)) as SB_Sales_SameSKU_Same_Share ,(SBV_Sales_OtherSKU_Same_Share + div0(ifnull(B.SBV_Sales_OtherSKU_ALLPRODUCTS,0),countasins)) as SBV_Sales_OtherSKU_Same_Share ,(SB_Sales_OtherSKU_Same_Share + div0(ifnull(B.SB_Sales_OtherSKU_ALLPRODUCTS,0),countasins)) as SB_Sales_OtherSKU_Same_Share ,(SBV_AD_NEWTOBRAND_ORDERS_Same_Share + div0(ifnull(B.SBV_AD_NEWTOBRAND_ORDERS_ALLPRODUCTS,0),countasins)) as SBV_AD_NEWTOBRAND_ORDERS_Same_Share ,(SB_AD_NEWTOBRAND_ORDERS_Same_Share + div0(ifnull(B.SB_AD_NEWTOBRAND_ORDERS_ALLPRODUCTS,0),countasins)) as SB_AD_NEWTOBRAND_ORDERS_Same_Share ,(SBV_AD_NEWTOBRAND_SALES_Same_Share + div0(ifnull(B.SBV_AD_NEWTOBRAND_SALES_ALLPRODUCTS,0),countasins)) as SBV_AD_NEWTOBRAND_SALES_Same_Share ,(SB_AD_NEWTOBRAND_SALES_Same_Share + div0(ifnull(B.SB_AD_NEWTOBRAND_SALES_ALLPRODUCTS,0),countasins)) as SB_AD_NEWTOBRAND_SALES_Same_Share ,(SBV_AD_NEWTOBRAND_UNITS_Same_Share + div0(ifnull(B.SBV_AD_NEWTOBRAND_UNITS_ALLPRODUCTS,0),countasins)) as SBV_AD_NEWTOBRAND_UNITS_Same_Share ,(SB_AD_NEWTOBRAND_UNITS_Same_Share + div0(ifnull(B.SB_AD_NEWTOBRAND_UNITS_ALLPRODUCTS,0),countasins)) as SB_AD_NEWTOBRAND_UNITS_Same_Share from SBSBVASIN_WOALLPRODUCT A full outer join SBSBVASIN_ALLPRODUCT B on A.date = B.date ) select coalesce(A.Date, B.Date) Date ,coalesce(A.ASIN, B.ASIN) ASIN ,SD_IMPRESSIONS ,SP_IMPRESSIONS ,SD_CLICKS ,SP_CLICKS ,SD_AD_conversions ,SP_AD_conversions ,SD_AD_spend ,SP_AD_spend ,SD_adsales ,SP_adsales ,SD_AD_conversionssamesku ,SP_AD_conversionssamesku ,SD_AD_conversionsothersku ,SP_AD_conversionsothersku ,SD_AD_salessamesku ,SP_AD_salessamesku ,SD_AD_OTHERSKUSALES ,SP_AD_OTHERSKUSALES ,SD_AD_newtobrandsales ,SP_AD_newtobrandsales ,SD_AD_newtobrandunits ,SP_AD_newtobrandunits ,SD_AD_newtobrandorders ,SP_AD_newtobrandorders ,SBV_Impression_Session_Share ,SB_Impression_Session_Share ,SBV_Clicks_Session_Share ,SB_Clicks_Session_Share ,SBV_Ad_Conversions_Session_Share ,SB_Ad_Conversions_Session_Share ,SBV_Spend_Session_Share ,SB_Spend_Session_Share ,SBV_Ad_Sales_Session_Share ,SB_Ad_Sales_Session_Share ,SBV_Ad_Conversions_Same_SKU_Session_Share ,SB_Ad_Conversions_Same_SKU_Session_Share ,SBV_ConversionOtherSKU_Session_Share ,SB_ConversionOtherSKU_Session_Share ,SBV_Sales_SameSKU_Session_Share ,SB_Sales_SameSKU_Session_Share ,SBV_Sales_OtherSKU_Session_Share ,SB_Sales_OtherSKU_Session_Share ,SBV_AD_NEWTOBRAND_ORDERS_Session_Share ,SB_AD_NEWTOBRAND_ORDERS_Session_Share ,SBV_AD_NEWTOBRAND_SALES_Session_Share ,SB_AD_NEWTOBRAND_SALES_Session_Share ,SBV_AD_NEWTOBRAND_UNITS_Session_Share ,SB_AD_NEWTOBRAND_UNITS_Session_Share ,SBV_Impressions_Same_Share ,SB_Impressions_Same_Share ,SBV_Clicks_Same_Share ,SB_Clicks_Same_Share ,SBV_Ad_Conversions_Same_Share ,SB_Ad_Conversions_Same_Share ,SBV_Spend_Same_Share ,SB_Spend_Same_Share ,SBV_Ad_Sales_Same_Share ,SB_Ad_Sales_Same_Share ,SBV_Ad_Conversions_Same_SKU_Same_Share ,SB_Ad_Conversions_Same_SKU_Same_Share ,SBV_ConversionOtherSKU_Same_Share ,SB_ConversionOtherSKU_Same_Share ,SBV_Sales_SameSKU_Same_Share ,SB_Sales_SameSKU_Same_Share ,SBV_Sales_OtherSKU_Same_Share ,SB_Sales_OtherSKU_Same_Share ,SBV_AD_NEWTOBRAND_ORDERS_Same_Share ,SB_AD_NEWTOBRAND_ORDERS_Same_Share ,SBV_AD_NEWTOBRAND_SALES_Same_Share ,SB_AD_NEWTOBRAND_SALES_Same_Share ,SBV_AD_NEWTOBRAND_UNITS_Same_Share ,SB_AD_NEWTOBRAND_UNITS_Same_Share ,(ifnull(SBV_Impressions_Same_Share,0) + ifnull(SB_Impressions_Same_Share,0) + ifnull(SD_IMPRESSIONS,0) + ifnull(SP_IMPRESSIONS,0)) as Total_IMPRESSIONS_Same_Share ,(ifnull(SBV_Clicks_Same_Share,0) + ifnull(SB_Clicks_Same_Share,0) + ifnull(SD_CLICKS,0) + ifnull(SP_CLICKS,0)) as Total_CLICKS_Same_Share ,(ifnull(SBV_Ad_Conversions_Same_Share,0) + ifnull(SB_Ad_Conversions_Same_Share,0) + ifnull(SD_AD_conversions,0) + ifnull(SP_AD_conversions,0)) as Total_AD_conversions_Same_Share ,(ifnull(SBV_Spend_Same_Share,0) + ifnull(SB_Spend_Same_Share,0) + ifnull(SD_AD_spend,0) + ifnull(SP_AD_spend,0)) as Total_AD_spend_Same_Share ,(ifnull(SBV_Ad_Sales_Same_Share,0) + ifnull(SB_Ad_Sales_Same_Share,0) + ifnull(SD_adsales,0) + ifnull(SP_adsales,0)) as Total_adsales_Same_Share ,(ifnull(SBV_Ad_Conversions_Same_SKU_Same_Share,0) + ifnull(SB_Ad_Conversions_Same_SKU_Same_Share,0) + ifnull(SD_AD_conversionssamesku,0) + ifnull(SP_AD_conversionssamesku,0)) as Total_AD_conversionssamesku_Same_Share ,(ifnull(SBV_ConversionOtherSKU_Same_Share,0) + ifnull(SB_ConversionOtherSKU_Same_Share,0) + ifnull(SD_AD_conversionsothersku,0) + ifnull(SP_AD_conversionsothersku,0)) as Total_AD_conversionsothersku_Same_Share ,(ifnull(SBV_Sales_SameSKU_Same_Share,0) + ifnull(SB_Sales_SameSKU_Same_Share,0) + ifnull(SD_AD_salessamesku,0) + ifnull(SP_AD_salessamesku,0)) as Total_AD_salessamesku_Same_Share ,(ifnull(SBV_Sales_OtherSKU_Same_Share,0) + ifnull(SB_Sales_OtherSKU_Same_Share,0) + ifnull(SD_AD_OTHERSKUSALES,0) + ifnull(SP_AD_OTHERSKUSALES,0)) as Total_AD_OTHERSKUSALES_Same_Share ,(ifnull(SBV_AD_NEWTOBRAND_ORDERS_Same_Share,0) + ifnull(SB_AD_NEWTOBRAND_ORDERS_Same_Share,0) + ifnull(SD_AD_newtobrandsales,0) + ifnull(SP_AD_newtobrandsales,0)) as Total_AD_newtobrandsales_Same_Share ,(ifnull(SBV_AD_NEWTOBRAND_SALES_Same_Share,0) + ifnull(SB_AD_NEWTOBRAND_SALES_Same_Share,0) + ifnull(SD_AD_newtobrandunits,0) + ifnull(SP_AD_newtobrandunits,0)) as Total_AD_newtobrandunits_Same_Share ,(ifnull(SBV_AD_NEWTOBRAND_UNITS_Same_Share,0) + ifnull(SB_AD_NEWTOBRAND_UNITS_Same_Share,0) + ifnull(SD_AD_newtobrandorders,0) + ifnull(SP_AD_newtobrandorders,0)) as Total_AD_newtobrandorders_Same_Share ,(ifnull(SBV_Impression_Session_Share,0) + ifnull(SB_Impression_Session_Share,0) + ifnull(SD_IMPRESSIONS,0) + ifnull(SP_IMPRESSIONS,0)) as Total_IMPRESSIONS_Session_Share ,(ifnull(SBV_Clicks_Session_Share,0) + ifnull(SB_Clicks_Session_Share,0) + ifnull(SD_CLICKS,0) + ifnull(SP_CLICKS,0)) as Total_CLICKS_Session_Share ,(ifnull(SBV_Ad_Conversions_Session_Share,0) + ifnull(SB_Ad_Conversions_Session_Share,0) + ifnull(SD_AD_conversions,0) + ifnull(SP_AD_conversions,0)) as Total_AD_conversions_Session_Share ,(ifnull(SBV_Spend_Session_Share,0) + ifnull(SB_Spend_Session_Share,0) + ifnull(SD_AD_spend,0) + ifnull(SP_AD_spend,0)) as Total_AD_spend_Session_Share ,(ifnull(SBV_Ad_Sales_Session_Share,0) + ifnull(SB_Ad_Sales_Session_Share,0) + ifnull(SD_adsales,0) + ifnull(SP_adsales,0)) as Total_adsales_Session_Share ,(ifnull(SBV_Ad_Conversions_Same_SKU_Session_Share,0) + ifnull(SB_Ad_Conversions_Same_SKU_Session_Share,0) + ifnull(SD_AD_conversionssamesku,0) + ifnull(SP_AD_conversionssamesku,0)) as Total_AD_conversionssamesku_Session_Share ,(ifnull(SBV_ConversionOtherSKU_Session_Share,0) + ifnull(SB_ConversionOtherSKU_Session_Share,0) + ifnull(SD_AD_conversionsothersku,0) + ifnull(SP_AD_conversionsothersku,0)) as Total_AD_conversionsothersku_Session_Share ,(ifnull(SBV_Sales_SameSKU_Session_Share,0) + ifnull(SB_Sales_SameSKU_Session_Share,0) + ifnull(SD_AD_salessamesku,0) + ifnull(SP_AD_salessamesku,0)) as Total_AD_salessamesku_Session_Share ,(ifnull(SBV_Sales_OtherSKU_Session_Share,0) + ifnull(SB_Sales_OtherSKU_Session_Share,0) + ifnull(SD_AD_OTHERSKUSALES,0) + ifnull(SP_AD_OTHERSKUSALES,0)) as Total_AD_OTHERSKUSALES_Session_Share ,(ifnull(SBV_AD_NEWTOBRAND_ORDERS_Session_Share,0) + ifnull(SB_AD_NEWTOBRAND_ORDERS_Session_Share,0) + ifnull(SD_AD_newtobrandsales,0) + ifnull(SP_AD_newtobrandsales,0)) as Total_AD_newtobrandsales_Session_Share ,(ifnull(SBV_AD_NEWTOBRAND_SALES_Session_Share,0) + ifnull(SB_AD_NEWTOBRAND_SALES_Session_Share,0) + ifnull(SD_AD_newtobrandunits,0) + ifnull(SP_AD_newtobrandunits,0)) as Total_AD_newtobrandunits_Session_Share ,(ifnull(SBV_AD_NEWTOBRAND_UNITS_Session_Share,0) + ifnull(SB_AD_NEWTOBRAND_UNITS_Session_Share,0) + ifnull(SD_AD_newtobrandorders,0) + ifnull(SP_AD_newtobrandorders,0)) as Total_AD_newtobrandorders_Session_Share from SPSDASIN A full outer join SBSBVASIN B on A.Date=B.Date and A.ASIN =B.ASIN;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from Perfora_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        