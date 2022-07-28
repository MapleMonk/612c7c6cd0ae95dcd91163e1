{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.AMAZON_ADS_US_HISTORICAL_SPONSORED_BRANDS__VIDEO_INTERMEDIATE AS SELECT ASIN, DATE, REPLACE(REPLACE (SPEND,\'$\',\'\'),\',\',\'\') AS SPEND, CLICKS, CURRENCY, IMPRESSIONS, \"Campaign Name\", \"Video Unmutes\", \"5 Second Views\", \"Portfolio name\", REPLACE(REPLACE(\"5 Second View Rate\",\'%\',\'\'),\',\',\'\') AS \"5 Second View Rate\", REPLACE(REPLACE(\"14 Day Total Sales \",\'$\',\'\'),\',\',\'\') AS \"14 Day Total Sales\", REPLACE(REPLACE(\"Cost Per Click (CPC)\",\'$\',\'\'),\',\',\'\') AS \"Cost Per Click (CPC)\", \"Video Complete Views\", \"Video Midpoint Views\", \"Viewable Impressions\", REPLACE(REPLACE(\"Click-Thru Rate (CTR)\",\'%\',\'\'),\',\',\'\') AS \"Click-Thru Rate (CTR)\", REPLACE(REPLACE(\"14 Day Conversion Rate\",\'%\',\'\'),\',\',\'\') AS \"14 Day Conversion Rate\", \"14 Day Total Units (#)\", \"14 Day Total Orders (#)\", REPLACE(REPLACE(\"View-Through Rate (VTR)\",\'%\',\'\'),\',\',\'\') AS \"View-Through Rate (VTR)\", \"Video First Quartile Views\", \"Video Third Quartile Views\", REPLACE(REPLACE(\"Click-Through Rate for Views (vCTR)\",\'%\',\'\'),\',\',\'\') AS \"Click-Through Rate for Views (vCTR)\", REPLACE(REPLACE(\"Total Advertising Cost of Sales (ACOS) \",\'%\',\'\'),\',\',\'\') AS \"Total Advertising Cost of Sales (ACOS)\", \"Total Return on Advertising Spend (ROAS)\", _AIRBYTE_AB_ID, _AIRBYTE_EMITTED_AT, _AIRBYTE_NORMALIZED_AT, _AIRBYTE_AA_US_HISTORICAL_SPONSORED_BRANDS___VIDEO_CAMPAI_HASHID FROM VAHDAM_DB.MAPLEMONK.AA_US_HISTORICAL_SPONSORED_BRANDS___VIDEO_CAMPAI; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.AMAZON_ADS_US_HISTORICAL_SPONSORED_DISPLAY_INTERMEDIATE AS select Date ,replace(replace(Spend, \'$\',\'\'),\',\',\'\') as Spend ,Clicks ,Currency ,\"Cost Type\" ,Impressions ,\"Ad Group Name\" ,\"Campaign Name\" ,\"Advertised SKU\" ,\"Portfolio name\" ,\"Advertised ASIN\" ,replace(replace(\"Cost Per Click (CPC)\", \'$\',\'\'),\',\',\'\') as \"Cost Per Click (CPC)\" ,replace(replace(\"14 Day Total Sales \", \'$\',\'\'),\',\',\'\') as \"14 Day Total Sales\" ,replace(replace(\"Click-Thru Rate (CTR)\", \'%\',\'\'),\',\',\'\') as \"Click-Thru Rate (CTR)\" ,replace(replace(\"14 Day Conversion Rate\", \'%\',\'\'),\',\',\'\') as \"14 Day Conversion Rate\" ,\"Viewable Impressions\" ,\"14 Day Total Units (#)\" ,\"14 Day Total Orders (#)\" ,replace(replace(\"14 Day New-to-brand Sales\",\'$\',\'\'),\',\',\'\') as \"14 Day New-to-brand Sales\" ,\"14 Day New-to-brand Units (#)\" ,\"14 Day New-to-brand Orders (#)\" ,\"14 Day Detail Page Views (DPV)\" ,\"Total Advertising Cost of Sales (ACOS) \" ,\"Total Return on Advertising Spend (ROAS)\" ,\"Cost per 1,000 viewable impressions (VCPM)\" ,_AIRBYTE_AB_ID ,_AIRBYTE_EMITTED_AT from \"VAHDAM_DB\".\"MAPLEMONK\".\"AA_US_HISTORICAL_SPONSORED_DISPLAY_ADVERTISED_PR\"; \CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.AMAZON_ADS_US_HISTORICAL_SPONSORED_PRODUCTS_INTERMEDIATE AS select Date ,replace(replace(Spend, \'$\',\'\'),\',\',\'\') as Spend ,Clicks ,Currency ,Impressions ,\"Ad Group Name\" ,\"Campaign Name\" ,\"Advertised SKU\" ,\"Portfolio name\" ,\"Advertised ASIN\" ,replace(replace(\"7 Day Total Sales \", \'$\',\'\'),\',\',\'\') as \"7 Day Total Sales\" ,replace(replace(\"Cost Per Click (CPC)\", \'$\',\'\'),\',\',\'\') as \"Cost Per Click (CPC)\" ,replace(replace(\"Click-Thru Rate (CTR)\", \'%\',\'\'),\',\',\'\') as \"Click-Thru Rate (CTR)\" ,replace(replace(\"7 Day Conversion Rate\", \'%\',\'\'),\',\',\'\') as \"7 Day Conversion Rate\" ,replace(replace(\"7 Day Other SKU Sales \", \'$\',\'\'),\',\',\'\') as \"7 Day Other SKU Sales\" ,replace(replace(\"7 Day Total Units (#)\", \'$\',\'\'),\',\',\'\') as \"7 Day Total Units (#)\" ,replace(replace(\"7 Day Total Orders (#)\", \'$\',\'\'),\',\',\'\') as \"7 Day Total Orders (#)\" ,\"7 Day Other SKU Units (#)\" ,replace(replace(\"7 Day Advertised SKU Sales \", \'$\',\'\'),\',\',\'\') as \"7 Day Advertised SKU Sales\" ,\"7 Day Advertised SKU Units (#)\" ,replace(replace(\"Total Advertising Cost of Sales (ACOS) \", \'%\',\'\'),\',\',\'\') as \"Total Advertising Cost of Sales (ACOS)\" ,\"Total Return on Advertising Spend (ROAS)\" ,_AIRBYTE_AB_ID ,_AIRBYTE_EMITTED_AT ,_AIRBYTE_NORMALIZED_AT ,_AIRBYTE_AA_US_HISTORICAL_SPONSORED_PRODUCT_ADVERTISED_PR_HASHID from \"VAHDAM_DB\".\"MAPLEMONK\".\"AA_US_HISTORICAL_SPONSORED_PRODUCT_ADVERTISED_PR\"; ---------------Sponsored Display Google Sheet-------------------- create or replace table vahdam_db.maplemonk.amazon_ads_us_sponsored_display_sponsored_products_temp as select Date ,replace(Spend, \'$\', \'\') as Spend ,Clicks ,Currency ,\"Cost Type\" ,Impressions ,\"Ad Group Name\" ,\"Campaign Name\" ,\"Advertised SKU\" ,\"Portfolio name\" ,\"Advertised ASIN\" ,\"Bid Optimization\" ,replace(replace(\"Cost Per Click (CPC)\", \'$\',\'\'),\',\',\'\') as \"Cost Per Click (CPC)\" ,replace(replace(\"14 Day Total Sales \", \'$\',\'\'),\',\',\'\') as \"14 Day Total Sales \" ,replace(replace(\"Click-Thru Rate (CTR)\", \'%\',\'\'),\',\',\'\') as \"Click-Thru Rate (CTR)\" ,\"Viewable Impressions\" ,\"14 Day Total Units (#)\" ,\"14 Day Total Orders (#)\" ,replace(replace(\"14 Day New-to-brand Sales\",\'$\',\'\'),\',\',\'\') as \"14 Day New-to-brand Sales\" ,replace(replace(\"14 Day Total Sales - (Click)\",\'$\',\'\'),\',\',\'\') as \"14 Day Total Sales - (Click)\" ,\"14 Day New-to-brand Units (#)\" ,\"14 Day New-to-brand Orders (#)\" ,\"14 Day Total Units (#) - (Click)\" ,\"14 Day Detail Page Views (DPV)\" ,\"14 Day Total Orders (#) - (Click)\" ,replace(replace(\"14 Day New-to-brand Sales - (Click)\", \'$\', \'\'), \',\',\'\') as \"Day_14_Newtobrand_Sales(Click)\" ,\"14 Day New-to-brand Units (#) - (Click)\" ,\"14 Day New-to-brand Orders (#) - (Click)\" ,\"Total Advertising Cost of Sales (ACOS) \" ,\"Total Return on Advertising Spend (ROAS)\" ,\"Cost per 1,000 viewable impressions (VCPM)\" ,\"Total Advertising Cost of Sales (ACOS) - (Click)\" ,\"Total Return on Advertising Spend (ROAS) - (Click)\" ,_AIRBYTE_AB_ID ,_AIRBYTE_EMITTED_AT from \"VAHDAM_DB\".\"MAPLEMONK\".\"AMAZON_ADS_US_SPONSORED_DISPLAY_ADVERTISED_PR\"; create or replace table \"VAHDAM_DB\".\"MAPLEMONK\".Amazon_Ads_US_sponsored_display_advertised_PR_intermediate as select * from ( select *, row_number() over (partition by concat(\"Campaign Name\", \"Ad Group Name\", \"Advertised SKU\", \"Advertised ASIN\", Date) order by _AIRBYTE_EMITTED_AT desc) row1 from vahdam_db.maplemonk.amazon_ads_us_sponsored_display_sponsored_products_temp ) where row1 = 1; create or replace table vahdam_db.maplemonk.Amazon_Ads_Sponsored_Products_Intermediate as select primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,campaignId ,campaignName ,adGroupId ,adGroupName ,campaignStatus ,keywordId ,keywordText ,adId ,matchType ,targetId ,targetingType ,targetingExpression ,targetingText ,currency ,asin ,otherAsin ,sku ,impressions ,clicks ,cost ,attributedSales14d ,attributedConversions14d ,attributedSales7d ,attributedConversions7d ,attributedConversions14dSameSKU ,attributedSales14dSameSKU ,attributedSales14dOtherSKU from ( select *,row_number()over(partition by primary_key order by _AIRBYTE_EMITTED_AT desc) as rw2 from (select array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int ,metric:campaignStatus::varchar,metric:keywordId::int,metric:matchType::varchar ,metric:targetId::int,metric:targetingType::varchar,metric:asin,metric:otherAsin::varchar,metric:sku::varchar)),\'\') primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,metric:campaignId::int as campaignId ,metric:campaignName::varchar as campaignName ,metric:adGroupId::int adGroupId ,metric:adGroupName::varchar as adGroupName ,metric:campaignStatus::varchar campaignStatus ,metric:keywordId::int keywordId ,metric:keywordText::varchar keywordText ,metric:adId::int adId ,metric:matchType::varchar matchType ,metric:targetId::int targetId ,metric:targetingType::varchar targetingType ,metric:targetingExpression::varchar targetingExpression ,metric:targetingText::varchar targetingText ,metric:currency::varchar currency ,metric:asin::varchar asin ,metric:otherAsin::varchar otherAsin ,metric:sku::varchar sku ,metric:impressions::float impressions ,metric:clicks::float clicks ,metric:cost::float cost ,metric:attributedSales14d::float attributedSales14d ,metric:attributedConversions14d::float attributedConversions14d ,metric:attributedSales7d::float attributedSales7d ,metric:attributedConversions7d::float attributedConversions7d ,metric:attributedConversions14dSameSKU::float attributedConversions14dSameSKU ,metric:attributedSales14dSameSKU::float attributedSales14dSameSKU ,metric:attributedSales14dOtherSKU::float attributedSales14dOtherSKU ,metric:impressions::float +metric:clicks::float +metric:cost::float +metric:attributedSales14d::float +metric:attributedConversions14d::float+metric:attributedSales7d::float +metric:attributedConversions7d::float as tt ,_AIRBYTE_EMITTED_AT ,row_number()over(partition by array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int ,metric:campaignStatus::varchar,metric:keywordId::int,metric:matchType::varchar ,metric:targetId::int,metric:targetingType::varchar,metric:asin,metric:otherAsin::varchar,metric:sku::varchar)),\'\') order by metric:impressions::float +metric:clicks::float +metric:cost::float +metric:attributedSales14d::float +metric:attributedConversions14d::float+metric:attributedSales7d::float +metric:attributedConversions7d::float desc) as rw from VAHDAM_DB.MAPLEMONK.AMAZON_ADS_NA_SPONSORED_PRODUCTS_REPORT_STREAM where PROFILEID in (\'1865822991259542\',\'4287412371422290\',\'2796474817192137\') and RECORDTYPE = \'productAds\' ) a where rw =1 or tt>0 ) b where rw2 =1; create or replace table vahdam_db.maplemonk.Amazon_Ads_Sponsored_Brands_Intermediate as select primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,campaignId ,campaignName ,adGroupId ,adGroupName ,campaignStatus ,campaignBudgetType ,adId ,keywordId ,keywordText ,keywordStatus ,matchType ,targetId ,targetingType ,targetingExpression ,targetingText ,currency ,asin ,otherAsin ,sku ,impressions ,clicks ,cost ,attributedSales14d ,attributedConversions14d ,attributedSales7d ,attributedConversions7d ,attributedOrdersNewToBrand14d ,attributedSalesNewToBrand14d ,attributedUnitsOrderedNewToBrand14d ,attributedConversions14dSameSKU ,attributedSales14dSameSKU ,attributedSales14dOtherSKU from ( select *,row_number()over(partition by primary_key order by _AIRBYTE_EMITTED_AT desc) as rw2 from (select array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int, metric:campaignBudgetType::varchar ,metric:keywordText::varchar, metric:matchType::varchar)),\'\') primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,metric:campaignId::int as campaignId ,metric:campaignName::varchar as campaignName ,metric:adGroupId::int adGroupId ,metric:adGroupName::varchar as adGroupName ,metric:campaignStatus::varchar campaignStatus ,metric:campaignBudgetType::varchar campaignBudgetType ,metric:adId::int adId ,metric:keywordText::varchar keywordText ,metric:keywordStatus::varchar keywordStatus ,metric:keywordId::int keywordId ,metric:matchType::varchar matchType ,metric:targetId::int targetId ,metric:targetingType::varchar targetingType ,metric:targetingExpression::varchar targetingExpression ,metric:targetingText::varchar targetingText ,metric:currency::varchar currency ,metric:asin::varchar asin ,metric:otherAsin::varchar otherAsin ,metric:sku::varchar sku ,metric:impressions::float impressions ,metric:clicks::float clicks ,metric:cost::float cost ,metric:attributedSales14d::float attributedSales14d ,metric:attributedConversions14d::float attributedConversions14d ,metric:attributedSales7d::float attributedSales7d ,metric:attributedConversions7d::float attributedConversions7d ,metric:attributedOrdersNewToBrand14d::float attributedOrdersNewToBrand14d ,metric:attributedSalesNewToBrand14d::float attributedSalesNewToBrand14d ,metric:attributedUnitsOrderedNewToBrand14d::float attributedUnitsOrderedNewToBrand14d ,metric:attributedConversions14dSameSKU::float attributedConversions14dSameSKU ,metric:attributedSales14dSameSKU::float attributedSales14dSameSKU ,metric:attributedSales14dOtherSKU::int attributedSales14dOtherSKU ,metric:impressions::float +metric:clicks::float +metric:cost::float +metric:attributedSales14d::float +metric:attributedConversions14d::float+metric:attributedSales7d::float +metric:attributedConversions7d::float as tt ,_AIRBYTE_EMITTED_AT ,row_number()over(partition by array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int, metric:campaignBudgetType::varchar ,metric:keywordText::varchar, metric:matchType::varchar)),\'\') order by metric:impressions::float +metric:clicks::float +metric:cost::float +metric:attributedSales14d::float +metric:attributedConversions14d::float+metric:attributedSales7d::float +metric:attributedConversions7d::float desc) as rw from VAHDAM_DB.MAPLEMONK.AMAZON_ADS_NA_SPONSORED_BRANDS_REPORT_STREAM where PROFILEID in (\'1865822991259542\',\'4287412371422290\',\'2796474817192137\',\'1466997913324443\') and RECORDTYPE = \'keywords\' ) a where rw =1 or tt>0 ) b where rw2 =1; create or replace table vahdam_db.maplemonk.Amazon_Ads_Sponsored_Brands_Video_Intermediate as select primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,campaignId ,campaignName ,adGroupId ,adGroupName ,campaignStatus ,campaignBudgetType ,adId ,keywordId ,keywordText ,keywordStatus ,matchType ,targetId ,targetingType ,targetingExpression ,targetingText ,currency ,asin ,otherAsin ,sku ,impressions ,clicks ,cost ,attributedSales14d ,attributedConversions14d ,attributedSales7d ,attributedConversions7d ,attributedConversions14dSameSKU ,attributedSales14dSameSKU ,attributedSales14dOtherSKU from ( select *,row_number()over(partition by primary_key order by _AIRBYTE_EMITTED_AT desc) as rw2 from (select array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int, metric:campaignBudgetType::varchar ,metric:keywordText::varchar, metric:matchType::varchar ,metric:targetingId::int, metric:targetingType::varchar)),\'\') primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,metric:campaignId::int as campaignId ,metric:campaignName::varchar as campaignName ,metric:adGroupId::int adGroupId ,metric:adGroupName::varchar as adGroupName ,metric:campaignStatus::varchar campaignStatus ,metric:campaignBudgetType::varchar campaignBudgetType ,metric:adId::int adId ,metric:keywordText::varchar keywordText ,metric:keywordStatus::varchar keywordStatus ,metric:keywordId::int keywordId ,metric:matchType::varchar matchType ,metric:targetId::int targetId ,metric:targetingType::varchar targetingType ,metric:targetingExpression::varchar targetingExpression ,metric:targetingText::varchar targetingText ,metric:currency::varchar currency ,metric:asin::varchar asin ,metric:otherAsin::varchar otherAsin ,metric:sku::varchar sku ,metric:impressions::float impressions ,metric:clicks::float clicks ,metric:cost::float cost ,metric:attributedSales14d::float attributedSales14d ,metric:attributedConversions14d::float attributedConversions14d ,metric:attributedSales7d::float attributedSales7d ,metric:attributedConversions7d::float attributedConversions7d ,metric:attributedConversions14dSameSKU::float attributedConversions14dSameSKU ,metric:attributedSales14dSameSKU::float attributedSales14dSameSKU ,metric:attributedSales14dOtherSKU::int attributedSales14dOtherSKU ,metric:impressions::float +metric:clicks::float +metric:cost::float +metric:attributedSales14d::float +metric:attributedConversions14d::float+metric:attributedSales7d::float +metric:attributedConversions7d::float as tt ,_AIRBYTE_EMITTED_AT ,row_number()over(partition by array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int, metric:campaignBudgetType::varchar ,metric:keywordText::varchar, metric:matchType::varchar ,metric:targetingId::int, metric:targetingType::varchar)),\'\') order by metric:impressions::float +metric:clicks::float +metric:cost::float +metric:attributedSales14d::float +metric:attributedConversions14d::float+metric:attributedSales7d::float +metric:attributedConversions7d::float desc) as rw from VAHDAM_DB.MAPLEMONK.AMAZON_ADS_NA_SPONSORED_BRANDS_VIDEO_REPORT_STREAM where PROFILEID in (\'1865822991259542\',\'4287412371422290\',\'2796474817192137\',\'1466997913324443\') and RECORDTYPE = \'keywords\' ) a where rw =1 or tt>0 ) b where rw2 =1; create or replace table vahdam_db.maplemonk.Amazon_Ads_Sponsored_Display_Intermediate as select primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,campaignId ,campaignName ,adGroupId ,adGroupName ,campaignStatus ,campaignBudgetType ,adId ,keywordId ,keywordText ,keywordStatus ,matchType ,targetId ,targetingType ,targetingExpression ,targetingText ,currency ,asin ,otherAsin ,sku ,impressions ,clicks ,cost ,attributedSales14d ,attributedConversions14d ,attributedSales7d ,attributedConversions7d ,attributedOrdersNewToBrand14d ,attributedSalesNewToBrand14d ,attributedUnitsOrderedNewToBrand14d ,attributedConversions14dSameSKU ,attributedSales14dSameSKU ,attributedSales14dOtherSKU from ( select *,row_number()over(partition by primary_key order by _AIRBYTE_EMITTED_AT desc) as rw2 from (select array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int, metric:adId::int, metric:targetId::int, metric:targetingType::varchar, metric:asin::varchar, metric:otherAsin::varchar, metric:sku::varchar)),\'\') primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,metric:campaignId::int as campaignId ,metric:campaignName::varchar as campaignName ,metric:adGroupId::int adGroupId ,metric:adGroupName::varchar as adGroupName ,metric:campaignStatus::varchar campaignStatus ,metric:campaignBudgetType::varchar campaignBudgetType ,metric:keywordText::varchar keywordText ,metric:keywordStatus::varchar keywordStatus ,metric:keywordId::int keywordId ,metric:matchType::varchar matchType ,metric:adId::int adId ,metric:targetId::int targetId ,metric:targetingType::varchar targetingType ,metric:targetingExpression::varchar targetingExpression ,metric:targetingText::varchar targetingText ,metric:currency::varchar currency ,metric:asin::varchar asin ,metric:otherAsin::varchar otherAsin ,metric:sku::varchar sku ,metric:impressions::float impressions ,metric:clicks::float clicks ,metric:cost::float cost ,metric:attributedSales14d::float attributedSales14d ,metric:attributedConversions14d::float attributedConversions14d ,metric:attributedSales7d::float attributedSales7d ,metric:attributedConversions7d::float attributedConversions7d ,metric:attributedOrdersNewToBrand14d::float attributedOrdersNewToBrand14d ,metric:attributedSalesNewToBrand14d::float attributedSalesNewToBrand14d ,metric:attributedUnitsOrderedNewToBrand14d::float attributedUnitsOrderedNewToBrand14d ,metric:attributedConversions14dSameSKU::float attributedConversions14dSameSKU ,metric:attributedSales14dSameSKU::float attributedSales14dSameSKU ,metric:attributedSales14dOtherSKU::float attributedSales14dOtherSKU ,metric:impressions::float +metric:clicks::float +metric:cost::float +metric:attributedSales14d::float +metric:attributedConversions14d::float+metric:attributedSales7d::float +metric:attributedConversions7d::float as tt ,_AIRBYTE_EMITTED_AT ,row_number()over(partition by array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int, metric:adId::int, metric:targetId::int, metric:targetingType::varchar, metric:asin::varchar, metric:otherAsin::varchar, metric:sku::varchar)),\'\') order by metric:impressions::float +metric:clicks::float +metric:cost::float +metric:attributedSales14d::float +metric:attributedConversions14d::float+metric:attributedSales7d::float +metric:attributedConversions7d::float desc) as rw from VAHDAM_DB.MAPLEMONK.AMAZON_ADS_NA_SPONSORED_DISPLAY_REPORT_STREAM where RECORDTYPE = \'productAds\' and PROFILEID in (\'1865822991259542\',\'4287412371422290\',\'2796474817192137\',\'1466997913324443\') ) a where rw =1 or tt>0 ) b where rw2 =1; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.AMAZONADS_NA_MARKETING_OLD AS with orders as ( select order_timestamp::date as order_date, product_id as asin, sum(net_sales) as sales, count(distinct order_id) as orders from VAHDAM_DB.MAPLEMONK.FACT_ITEMS where shop_name = \'Amazon_USA\' group by 1,2 ), amazon as ( SELECT \'Sponsored Products\' CAMPAIGN_TYPE, sp.PROFILEID ,TO_DATE(sp.REPORTDATE,\'YYYYMMDD\') - 1 AS Date ,sp.CampaignId ,sp.campaignName ,sp.adGroupId ,sp.adGroupName ,sp.keywordText ,sp.campaignStatus ,sp.ASIN as Asin ,sp.adId ,sp.targetingExpression ,sp.targetingText ,sp.currency ,Null as NewToBrandOrders ,Null as NewToBrandSales ,Null as NewToBrandUnits ,sum(sp.impressions) as Impressions ,sum(sp.clicks) as Clicks ,sum(sp.cost) as Spend ,sum(sp.attributedSales7d) as Sales ,sum(sp.attributedConversions7d) as Conversions ,sum(sp.attributedConversions14dSameSKU) as ConversionsSameSKU ,sum(sp.attributedSales14dSameSKU) as SalesSameSKU ,sum(sp.attributedSales14dOtherSKU) as OtherSKUSales ,(sum(sp.attributedConversions14d) - sum(sp.attributedConversions14dSameSKU)) as ConversionsOtherSKU FROM vahdam_db.maplemonk.Amazon_Ads_Sponsored_Products_Intermediate sp WHERE sp.RECORDTYPE = \'productAds\' and sp.PROFILEID in (\'1865822991259542\',\'4287412371422290\',\'2796474817192137\') GROUP BY sp.REPORTDATE, sp.PROFILEID, sp.campaignId, sp.campaignName, sp.adGroupId, sp.adGroupName, sp.keywordText ,sp.campaignStatus, sp.ASIN, sp.adId, sp.targetingExpression, sp.targetingText, sp.targetingType, sp.currency UNION SELECT \'Sponsored Display\' CAMPAIGN_TYPE, sd.PROFILEID ,TO_DATE(sd.REPORTDATE,\'YYYYMMDD\') - 1 AS Date ,sd.campaignId as CampaignID ,sd.campaignName as CampaignName ,sd.adGroupId as AdGroupId ,sd.adGroupName as AdGroupName ,sd.keywordText as Keywordtext ,sd.campaignStatus as CampaignStatus ,sd.asin as Asin ,sd.adId as AdId ,sd.targetingExpression as TargetingExpression ,sd.targetingText as TargetingText ,sd.currency as Currency ,sum(sd.attributedOrdersNewToBrand14d) as NewToBrandOrders ,sum(sd.attributedSalesNewToBrand14d) as NewToBrandSales ,sum(sd.attributedUnitsOrderedNewToBrand14d) as NewToBrandUnits ,sum(sd.impressions) as Impressions ,sum(sd.clicks) as Clicks ,sum(sd.cost) as Spend ,sum(sd.attributedSales7d) as Sales ,sum(sd.attributedConversions7d) as Conversions ,sum(sd.attributedConversions14dSameSKU) as ConversionsSameSKU ,sum(sd.attributedSales14dSameSKU) as SalesSameSKU ,sum(sd.attributedSales14dOtherSKU) as OtherSKUSales ,(sum(sd.attributedConversions14d) - sum(sd.attributedConversions14dSameSKU)) as ConversionsOtherSKU FROM vahdam_db.maplemonk.Amazon_Ads_Sponsored_Display_Intermediate sd WHERE sd.RECORDTYPE = \'productAds\' and sd.PROFILEID in (\'1865822991259542\',\'4287412371422290\',\'2796474817192137\',\'1466997913324443\') GROUP BY sd.REPORTDATE, sd.PROFILEID, sd.campaignId, sd.campaignName, sd.adGroupId, sd.adGroupName, sd.keywordText, sd.campaignStatus ,sd.ASIN, sd.adId, sd.targetingExpression, sd.targetingText, sd.targetingType, sd.currency UNION SELECT \'Sponsored Brands\' CAMPAIGN_TYPE, sb.PROFILEID ,TO_DATE(sb.REPORTDATE,\'YYYYMMDD\') - 1 AS Date ,sb.campaignId as CampaignID ,sb.campaignName as CampaignName ,sb.adGroupId as AdGroupId ,sb.adGroupName as AdGroupName ,sb.keywordText as Keywordtext ,sb.campaignStatus as CampaignStatus ,sb.asin as Asin ,sb.adId as AdId ,sb.targetingExpression as TargetingExpression ,sb.targetingText as TargetingText ,sb.currency as Currency ,sum(sb.attributedOrdersNewToBrand14d) as NewToBrandOrders ,sum(sb.attributedSalesNewToBrand14d) as NewToBrandSales ,sum(sb.attributedUnitsOrderedNewToBrand14d) as NewToBrandUnits ,sum(sb.impressions) as Impressions ,sum(sb.clicks) as Clicks ,sum(sb.cost) as Spend ,sum(sb.attributedSales14d) as Sales ,sum(sb.attributedConversions14d) as Conversions ,sum(sb.attributedConversions14dSameSKU) as ConversionsSameSKU ,sum(sb.attributedSales14dSameSKU) as SalesSameSKU ,sum(sb.attributedSales14dOtherSKU) as SalesOtherSKU ,(sum(sb.attributedConversions14d) - sum(sb.attributedConversions14dSameSKU)) as ConversionsOtherSKU FROM vahdam_db.maplemonk.Amazon_Ads_Sponsored_Brands_Intermediate sb WHERE sb.RECORDTYPE = \'keywords\' and sb.PROFILEID in (\'1865822991259542\',\'4287412371422290\',\'2796474817192137\',\'1466997913324443\') GROUP BY sb.REPORTDATE, sb.PROFILEID, sb.campaignId, sb.campaignName, sb.adGroupId, sb.adGroupName, sb.keywordText ,sb.campaignStatus, sb.ASIN, sb.adId, sb.targetingExpression, sb.targetingText, sb.targetingType, sb.currency UNION SELECT \'Sponsored Brands Video\' CAMPAIGN_TYPE, sbv.PROFILEID ,TO_DATE(sbv.REPORTDATE,\'YYYYMMDD\') - 1 AS Date ,sbv.campaignId as CampaignID ,sbv.campaignName as CampaignName ,sbv.adGroupId as AdGroupId ,sbv.adGroupName as AdGroupName ,sbv.keywordText as Keywordtext ,sbv.campaignStatus as CampaignStatus ,sbv.asin as Asin ,sbv.adId as AdId ,sbv.targetingExpression as TargetingExpression ,sbv.targetingText as TargetingText ,sbv.currency as Currency ,Null as NewToBrandOrders ,Null as NewToBrandSales ,Null as NewToBrandUnits ,sum(sbv.impressions) as Impressions ,sum(sbv.clicks) as Clicks ,sum(sbv.cost) as Spend ,sum(sbv.attributedSales14d) as Sales ,sum(sbv.attributedConversions14d) as Conversions ,sum(sbv.attributedConversions14dSameSKU) as ConversionsSameSKU ,sum(sbv.attributedSales14dSameSKU) as SalesSameSKU ,sum(sbv.attributedSales14dOtherSKU) as SalesOtherSKU ,(sum(sbv.attributedConversions14d) - sum(sbv.attributedConversions14dSameSKU)) as ConversionsOtherSKU FROM vahdam_db.maplemonk.Amazon_Ads_Sponsored_Brands_Video_Intermediate sbv WHERE sbv.RECORDTYPE = \'keywords\' and sbv.PROFILEID in (\'1865822991259542\',\'4287412371422290\',\'2796474817192137\',\'1466997913324443\') GROUP BY sbv.REPORTDATE, sbv.PROFILEID, sbv.campaignId, sbv.campaignName, sbv.adGroupId, sbv.adGroupName , sbv.keywordText, sbv.campaignStatus, sbv.ASIN, sbv.adId, sbv.targetingExpression, sbv.targetingText ,sbv.targetingType, sbv.currency union select \'Sponsored Display\' as CAMPAIGN_TYPE ,NULL as PROFILEID ,TO_DATE(SDI.DATE, \'Mon dd, yyyy\') as DATE ,NULL as CAMPAIGNID ,SDI.\"Campaign Name\" AS CAMPAIGNNAME ,NULL AS ADGROUPID ,SDI.\"Ad Group Name\" AS ADGROUPNAME ,NULL AS KEYWORDTEXT ,NULL AS CAMPAIGNSTATUS ,SDI.\"Advertised ASIN\" AS ASIN ,NULL AS ADID ,NULL AS TARGETINGEXPRESSION ,NULL AS TARGETINGTEXT ,SDI.Currency AS CURRENCY ,SUM(SDI.\"14 Day New-to-brand Orders (#)\") AS NEWTOBRANDORDERS ,SUM(SDI.\"14 Day New-to-brand Sales\") AS NEWTOBRANDSALES ,SUM(SDI.\"14 Day New-to-brand Units (#)\") AS NEWTOBRANDUNITS ,SUM(SDI.Impressions) AS IMPRESSIONS ,SUM(SDI.Clicks) AS CLICKS ,SUM(SDI.SPEND) AS SPEND ,SUM(SDI.\"14 Day Total Sales \") AS SALES ,SUM(SDI.\"14 Day Total Orders (#)\") AS CONVERSIONS ,NULL AS CONVERSIONSSAMESKU ,NULL AS SALESSAMESKU ,NULL AS OTHERSKUSALES ,NULL AS CONVERSIONSOTHERSKU FROM VAHDAM_DB.MAPLEMONK.AMAZON_ADS_US_SPONSORED_DISPLAY_ADVERTISED_PR_INTERMEDIATE SDI where lower(\"Campaign Name\") like \'%vr%\' or (lower(\"Campaign Name\") like \'%purchase remarketing%\' and \"Campaign Name\" not like \'SD - Purchase Remarketing Green Tea | MR\') GROUP BY TO_DATE(SDI.DATE, \'Mon dd, yyyy\'), SDI.\"Campaign Name\", SDI.\"Ad Group Name\", SDI.\"Advertised ASIN\", SDI.Currency UNION select \'Sponsored Product\' as Campaign_Type, null as ProfileId, TO_DATE(HSP.DATE, \'Mon dd, yyyy\') as DATE, null as campaignid, HSP.\"Campaign Name\" as CampaignName, null as adgroupid, HSP.\"Ad Group Name\" as AdGroupName, null as KeywordText, null as CampaignStatus, HSP.\"Advertised ASIN\" as ASIN, null as adid, null as TargetingExpression, null as TargetingText, HSP.Currency, null as NEWTOBRANDORDERS, null as NEWTOBRANDSALES, null as NEWTOBRANDUNITS, sum(HSP.impressions) as Impressions, sum(HSP.clicks) as Clicks, sum(HSP.spend) as Spend, sum(HSP.\"7 Day Total Sales\") as Sales, sum(HSP.\"7 Day Total Orders (#)\") as Conversions, null as CONVERSIONSSAMESKU, sum(HSP.\"7 Day Advertised SKU Sales\") as SALESSAMESKU, sum(HSP.\"7 Day Other SKU Sales\") as OTHERSKUSALES, null as CONVERSIONSOTHERSKU from VAHDAM_DB.MAPLEMONK.AMAZON_ADS_US_HISTORICAL_SPONSORED_PRODUCTS_INTERMEDIATE HSP GROUP BY TO_DATE(HSP.DATE, \'Mon dd, yyyy\'), HSP.\"Campaign Name\", HSP.\"Ad Group Name\", HSP.\"Advertised ASIN\", HSP.Currency UNION select \'Sponsored Display\' as CAMPAIGN_TYPE ,NULL as PROFILEID ,TO_DATE(HSD.DATE, \'Mon dd, yyyy\') as DATE ,NULL as CAMPAIGNID ,HSD.\"Campaign Name\" AS CAMPAIGNNAME ,NULL AS ADGROUPID ,HSD.\"Ad Group Name\" AS ADGROUPNAME ,NULL AS KEYWORDTEXT ,NULL AS CAMPAIGNSTATUS ,HSD.\"Advertised ASIN\" AS ASIN ,NULL AS ADID ,NULL AS TARGETINGEXPRESSION ,NULL AS TARGETINGTEXT ,HSD.Currency AS CURRENCY ,SUM(HSD.\"14 Day New-to-brand Orders (#)\") AS NEWTOBRANDORDERS ,SUM(HSD.\"14 Day New-to-brand Sales\") AS NEWTOBRANDSALES ,SUM(HSD.\"14 Day New-to-brand Units (#)\") AS NEWTOBRANDUNITS ,SUM(HSD.Impressions) AS IMPRESSIONS ,SUM(HSD.Clicks) AS CLICKS ,SUM(HSD.SPEND) AS SPEND ,SUM(HSD.\"14 Day Total Sales\") AS SALES ,SUM(HSD.\"14 Day Total Orders (#)\") AS CONVERSIONS ,NULL AS CONVERSIONSSAMESKU ,NULL AS SALESSAMESKU ,NULL AS OTHERSKUSALES ,NULL AS CONVERSIONSOTHERSKU FROM VAHDAM_DB.MAPLEMONK.AMAZON_ADS_US_HISTORICAL_SPONSORED_DISPLAY_INTERMEDIATE HSD GROUP BY TO_DATE(HSD.DATE, \'Mon dd, yyyy\'), HSD.\"Campaign Name\", HSD.\"Ad Group Name\", HSD.\"Advertised ASIN\", HSD.Currency UNION select \'Sponsored Brands + Video\' as CAMPAIGN_TYPE ,NULL as PROFILEID ,TO_DATE(HSB.DATE, \'Mon dd, yyyy\') as DATE ,NULL as CAMPAIGNID ,HSB.\"Campaign Name\" AS CAMPAIGNNAME ,NULL AS ADGROUPID ,NULL AS ADGROUPNAME ,NULL AS KEYWORDTEXT ,NULL AS CAMPAIGNSTATUS ,HSB.ASIN AS ASIN ,NULL AS ADID ,NULL AS TARGETINGEXPRESSION ,NULL AS TARGETINGTEXT ,HSB.Currency AS CURRENCY ,NULL AS NEWTOBRANDORDERS ,NULL AS NEWTOBRANDSALES ,NULL AS NEWTOBRANDUNITS ,SUM(HSB.Impressions) AS IMPRESSIONS ,SUM(HSB.Clicks) AS CLICKS ,SUM(HSB.SPEND) AS SPEND ,SUM(HSB.\"14 Day Total Sales\") AS SALES ,SUM(HSB.\"14 Day Total Orders (#)\") AS CONVERSIONS ,NULL AS CONVERSIONSSAMESKU ,NULL AS SALESSAMESKU ,NULL AS OTHERSKUSALES ,NULL AS CONVERSIONSOTHERSKU FROM VAHDAM_DB.MAPLEMONK.AMAZON_ADS_US_HISTORICAL_SPONSORED_BRANDS__VIDEO_INTERMEDIATE HSB GROUP BY TO_DATE(HSB.DATE, \'Mon dd, yyyy\'), HSB.\"Campaign Name\", HSB.ASIN, HSB.Currency ) select a.*,o.Sales/count(1)over(partition by a.asin,a.date) as Sales_usd, o.orders/count(1)over(partition by a.asin,a.date) as orders from amazon a left join orders o on a.asin = o.asin and order_date::date = a.Date::date where a.Date::date < \'2022-05-23\';",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from VAHDAM_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        