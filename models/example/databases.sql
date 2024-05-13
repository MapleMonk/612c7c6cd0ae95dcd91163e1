{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table assembly_db.MAPLEMONK.assembly_db_AA_Sponsored_Products_Intermediate as select primary_key ,TO_DATE(REPORTDATE,\'YYYY-MM-DD\') AS Date ,RECORDTYPE ,PROFILEID ,campaignId ,campaignName ,adGroupId ,adGroupName ,campaignStatus ,keywordId ,keywordText ,adId ,matchType ,targetId ,targetingType ,targetingExpression ,targetingText ,currency ,asin ,otherAsin ,sku ,impressions ,clicks ,cost ,attributedSales14d ,attributedConversions14d ,attributedSales7d ,attributedConversions7d ,attributedConversions14dSameSKU ,attributedSales14dSameSKU ,attributedSales14dOtherSKU ,\'EU\' as region from ( select * ,row_number()over(partition by primary_key order by _AIRBYTE_EMITTED_AT desc) as rw2 from (select array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int,metric:adId::int,metric:campaignStatus::varchar,metric:keywordId::int,metric:matchType::varchar,metric:targetId::int,metric:targetingType::varchar,metric:advertisedAsin::varchar,metric:purchasedAsin::varchar,metric:advertisedSku::varchar)),\'\') primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,metric:campaignId::int as campaignId ,metric:campaignName::varchar as campaignName ,metric:adGroupId::int adGroupId ,metric:adGroupName::varchar as adGroupName ,metric:campaignStatus::varchar campaignStatus ,metric:keywordId::int keywordId ,metric:keyword::varchar keywordText ,metric:adId::int adId ,metric:matchType::varchar matchType ,NULL targetId ,NULL targetingType ,NULL targetingExpression ,NULL targetingText ,\'INR\' currency ,metric:advertisedAsin::varchar asin ,metric:purchasedAsin::varchar otherAsin ,metric:advertisedSku::varchar sku ,metric:impressions::float impressions ,metric:clicks::float clicks ,metric:cost::float cost ,metric:sales14d::float attributedSales14d ,metric:purchases14d::float attributedConversions14d ,metric:sales7d::float attributedSales7d ,metric:purchases7d::float attributedConversions7d ,metric:purchasesSameSku14d::float attributedConversions14dSameSKU ,metric:attributedSalesSameSku14d::float attributedSales14dSameSKU ,metric:salesOtherSku14d::float attributedSales14dOtherSKU ,IFNULL(metric:impressions::float,0)+ IFNULL(metric:clicks::float,0)+ IFNULL(metric:cost::float,0) + IFNULL(metric:sales14d::float,0) + IFNULL(metric:purchases14d::float,0)+ IFNULL(metric:sales7d::float,0) + IFNULL(metric:purchases7d::float,0) as tt ,_AIRBYTE_EMITTED_AT ,row_number() over (partition by array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int,metric:adId::int ,metric:campaignStatus::varchar,metric:keywordId::int,metric:matchType::varchar,metric:advertisedAsin,metric:purchasedAsin::varchar,metric:advertisedSku::varchar)),\'\') order by IFNULL(metric:impressions::float,0)+ IFNULL(metric:clicks::float,0)+ IFNULL(metric:cost::float,0) + IFNULL(metric:sales14d::float,0) + IFNULL(metric:purchases14d::float,0)+ IFNULL(metric:sales7d::float,0) + IFNULL(metric:purchases7d::float,0) desc) as rw from assembly_db.MAPLEMONK.Amazon_Advertisement_Amazon_Ads_Assembly_SPONSORED_PRODUCTS_REPORT_STREAM where RECORDTYPE = \'productAds\' ) a where rw =1 or tt>0 ) b where rw2 =1 ; create or replace table assembly_db.MAPLEMONK.assembly_db_AA_Sponsored_Brands_Intermediate as select primary_key ,TO_DATE(REPORTDATE,\'YYYY-MM-DD\') AS Date ,RECORDTYPE ,PROFILEID ,campaignId ,campaignName ,adGroupId ,adGroupName ,campaignStatus ,campaignBudgetType ,adId ,keywordId ,keywordText ,keywordStatus ,matchType ,targetId ,targetingType ,targetingExpression ,targetingText ,currency ,asin ,otherAsin ,sku ,impressions ,clicks ,cost ,attributedSales14d ,attributedConversions14d ,attributedSales7d ,attributedConversions7d ,attributedOrdersNewToBrand14d ,attributedSalesNewToBrand14d ,attributedUnitsOrderedNewToBrand14d ,attributedConversions14dSameSKU ,attributedSales14dSameSKU ,attributedSales14dOtherSKU ,\'EU\' as region from ( select * ,row_number()over(partition by primary_key order by _AIRBYTE_EMITTED_AT desc) as rw2 from (select array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int,metric:campaignBudgetType::varchar,metric:keywordId::int,metric:keywordText::varchar, metric:matchType::varchar)),\'\') primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,metric:campaignId::int as campaignId ,metric:campaignName::varchar as campaignName ,metric:adGroupId::int adGroupId ,metric:adGroupName::varchar as adGroupName ,metric:campaignStatus::varchar campaignStatus ,metric:campaignBudgetType::varchar campaignBudgetType ,NULL adId ,metric:keywordText::varchar keywordText ,metric:keywordStatus::varchar keywordStatus ,metric:keywordId::int keywordId ,metric:matchType::varchar matchType ,NULL targetId ,NULL targetingType ,NULL targetingExpression ,NULL targetingText ,\'INR\' currency ,NULL asin ,NULL otherAsin ,NULL sku ,metric:impressions::float impressions ,metric:clicks::float clicks ,metric:cost::float cost ,metric:sales::float attributedSales14d ,metric:purchases::float attributedConversions14d ,NULL attributedSales7d ,NULL attributedConversions7d ,metric:newToBrandPurchases14d::float attributedOrdersNewToBrand14d ,metric:newToBrandSales14d::float attributedSalesNewToBrand14d ,metric:newToBrandUnitsSold14d::float attributedUnitsOrderedNewToBrand14d ,null attributedConversions14dSameSKU ,null attributedSales14dSameSKU ,null attributedSales14dOtherSKU ,IFNULL(metric:impressions::float,0) +IFNULL(metric:clicks::float,0) +IFNULL(metric:cost::float,0) +IFNULL(metric:sales::float,0) +IFNULL(metric:purchases::float,0) as tt ,_AIRBYTE_EMITTED_AT ,row_number() over (partition by array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int,metric:campaignBudgetType::varchar,metric:keywordId::int,metric:keywordText::varchar, metric:matchType::varchar)),\'\') order by IFNULL(metric:impressions::float,0) + IFNULL(metric:clicks::float,0) + IFNULL(metric:cost::float,0) + IFNULL(metric:sales::float,0) + IFNULL(metric:purchases::float,0) desc) as rw from assembly_db.MAPLEMONK.Amazon_Advertisement_Amazon_Ads_Assembly_SPONSORED_BRANDS_REPORT_STREAM_V3_MM where RECORDTYPE = \'keywords\' ) a where rw =1 or tt>0 ) b where rw2 =1; ; create or replace table assembly_db.MAPLEMONK.assembly_db_AA_Sponsored_Display_Intermediate as select primary_key ,TO_DATE(REPORTDATE,\'YYYYMMDD\') AS Date ,RECORDTYPE ,PROFILEID ,campaignId ,campaignName ,adGroupId ,adGroupName ,campaignStatus ,campaignBudgetType ,adId ,keywordId ,keywordText ,keywordStatus ,matchType ,targetId ,targetingType ,targetingExpression ,targetingText ,currency ,asin ,otherAsin ,sku ,impressions ,clicks ,cost ,attributedSales14d ,attributedConversions14d ,attributedSales7d ,attributedConversions7d ,attributedOrdersNewToBrand14d ,attributedSalesNewToBrand14d ,attributedUnitsOrderedNewToBrand14d ,attributedConversions14dSameSKU ,attributedSales14dSameSKU ,attributedSales14dOtherSKU ,\'EU\' as region from ( select * ,row_number()over(partition by primary_key order by _AIRBYTE_EMITTED_AT desc) as rw2 from (select array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int,metric:adId::int, metric:targetId::int, metric:targetingType::varchar, metric:asin::varchar, metric:otherAsin::varchar, metric:sku::varchar)),\'\') primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,metric:campaignId::int as campaignId ,metric:campaignName::varchar as campaignName ,metric:adGroupId::int adGroupId ,metric:adGroupName::varchar as adGroupName ,metric:campaignStatus::varchar campaignStatus ,NULL campaignBudgetType ,NULL keywordText ,NULL keywordStatus ,NULL keywordId ,NULL matchType ,metric:adId::int adId ,metric:targetId::int targetId ,metric:targetingType::varchar targetingType ,metric:targetingExpression::varchar targetingExpression ,metric:targetingText::varchar targetingText ,\'INR\' currency ,metric:asin::varchar asin ,metric:otherAsin::varchar otherAsin ,metric:sku::varchar sku ,metric:impressions::float impressions ,metric:clicks::float clicks ,metric:cost::float cost ,metric:attributedSales14d::float attributedSales14d ,metric:attributedConversions14d::float attributedConversions14d ,metric:attributedSales7d::float attributedSales7d ,metric:attributedConversions7d::float attributedConversions7d ,metric:attributedOrdersNewToBrand14d::float attributedOrdersNewToBrand14d ,metric:attributedSalesNewToBrand14d::float attributedSalesNewToBrand14d ,metric:attributedUnitsOrderedNewToBrand14d::float attributedUnitsOrderedNewToBrand14d ,metric:attributedConversions14dSameSKU::float attributedConversions14dSameSKU ,metric:attributedSales14dSameSKU::float attributedSales14dSameSKU ,metric:attributedSales14dOtherSKU::float attributedSales14dOtherSKU ,IFNULL(metric:impressions::float,0) + IFNULL(metric:clicks::float,0) + IFNULL(metric:cost::float,0) + IFNULL(metric:attributedSales14d::float,0) + IFNULL(metric:attributedConversions14d::float,0) + IFNULL(metric:attributedSales7d::float,0) + IFNULL(metric:attributedConversions7d::float,0) as tt ,_AIRBYTE_EMITTED_AT ,row_number()over(partition by array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int,metric:adId::int,metric:targetId::int, metric:targetingType::varchar, metric:asin::varchar, metric:otherAsin::varchar, metric:sku::varchar)),\'\') order by IFNULL(metric:impressions::float,0) + IFNULL(metric:clicks::float,0) + IFNULL(metric:cost::float,0) + IFNULL(metric:attributedSales14d::float,0) + IFNULL(metric:attributedConversions14d::float,0) + IFNULL(metric:attributedSales7d::float,0) + IFNULL(metric:attributedConversions7d::float,0) desc) as rw from assembly_db.MAPLEMONK.Amazon_Advertisement_Amazon_Ads_Assembly_SPONSORED_DISPLAY_REPORT_STREAM where RECORDTYPE = \'productAds\' ) a where rw =1 or tt>0 ) b where rw2 =1 ; CREATE OR REPLACE TABLE assembly_db.MAPLEMONK.assembly_db_AMAZONADS_MARKETING AS with amazonAds as ( SELECT \'Sponsored Products\' CAMPAIGN_TYPE ,sp.PROFILEID ,DATE ,sp.CampaignId ,sp.campaignName ,sp.adGroupId ,sp.adGroupName ,sp.keywordText ,sp.campaignStatus ,sp.ASIN as Asin ,sp.adId ,sp.targetingExpression ,sp.targetingText ,sp.currency ,sp.region ,Null as NewToBrandOrders ,Null as NewToBrandSales ,Null as NewToBrandUnits ,sum(sp.impressions) as Impressions ,sum(sp.clicks) as Clicks ,sum(sp.cost) as Spend ,sum(sp.attributedSales7d) as AdSales ,sum(sp.attributedConversions7d) as Conversions ,sum(sp.attributedConversions14dSameSKU) as ConversionsSameSKU ,sum(sp.attributedSales14dSameSKU) as SalesSameSKU ,sum(sp.attributedSales14dOtherSKU) as OtherSKUSales ,(sum(sp.attributedConversions14d) - sum(sp.attributedConversions14dSameSKU)) as ConversionsOtherSKU FROM assembly_db.MAPLEMONK.assembly_db_AA_Sponsored_Products_Intermediate sp WHERE sp.RECORDTYPE = \'productAds\' and not (ifnull(cost,0) = 0 and ifnull(Impressions,0) = 0 and ifnull(Clicks,0) = 0 and ifnull(attributedSales7d,0) = 0 and ifnull(attributedConversions7d,0) = 0 and ifnull(attributedConversions14dSameSKU,0) = 0 and ifnull(attributedSales14dSameSKU,0) = 0 and ifnull(attributedSales14dOtherSKU,0) = 0 and ifnull(attributedConversions14d,0) = 0) GROUP BY sp.DATE, sp.PROFILEID, sp.campaignId, sp.campaignName, sp.adGroupId, sp.adGroupName, sp.keywordText, sp.campaignStatus, sp.ASIN ,sp.adId, sp.targetingExpression, sp.targetingText, sp.targetingType, sp.currency, sp.region UNION SELECT \'Sponsored Display\' CAMPAIGN_TYPE ,sd.PROFILEID ,Date ,sd.campaignId as CampaignID ,sd.campaignName as CampaignName ,sd.adGroupId as AdGroupId ,sd.adGroupName as AdGroupName ,sd.keywordText as Keywordtext ,sd.campaignStatus as CampaignStatus ,sd.asin as Asin ,sd.adId as AdId ,sd.targetingExpression as TargetingExpression ,sd.targetingText as TargetingText ,sd.currency as Currency ,sd.region ,sum(sd.attributedOrdersNewToBrand14d) as NewToBrandOrders ,sum(sd.attributedSalesNewToBrand14d) as NewToBrandSales ,sum(sd.attributedUnitsOrderedNewToBrand14d) as NewToBrandUnits ,sum(sd.impressions) as Impressions ,sum(sd.clicks) as Clicks ,sum(sd.cost) as Spend ,sum(sd.attributedSales7d) as AdSales ,sum(sd.attributedConversions7d) as Conversions ,sum(sd.attributedConversions14dSameSKU) as ConversionsSameSKU ,sum(sd.attributedSales14dSameSKU) as SalesSameSKU ,sum(sd.attributedSales14dOtherSKU) as OtherSKUSales ,(sum(sd.attributedConversions14d) - sum(sd.attributedConversions14dSameSKU)) as ConversionsOtherSKU FROM assembly_db.MAPLEMONK.assembly_db_AA_Sponsored_Display_Intermediate sd WHERE sd.RECORDTYPE = \'productAds\' and not (ifnull(cost,0) = 0 and ifnull(Impressions,0) = 0 and ifnull(Clicks,0) = 0 and ifnull(attributedSales7d,0) = 0 and ifnull(attributedConversions7d,0) = 0 and ifnull(attributedConversions14dSameSKU,0) = 0 and ifnull(attributedSales14dSameSKU,0) = 0 and ifnull(attributedSales14dOtherSKU,0) = 0 and ifnull(attributedConversions14d,0) = 0 and ifnull(attributedOrdersNewToBrand14d,0) = 0 and ifnull(attributedSalesNewToBrand14d,0) = 0 and ifnull(attributedUnitsOrderedNewToBrand14d,0) = 0) GROUP BY sd.DATE, sd.PROFILEID, sd.campaignId, sd.campaignName, sd.adGroupId, sd.adGroupName, sd.keywordText, sd.campaignStatus, sd.ASIN, sd.adId ,sd.targetingExpression, sd.targetingText, sd.targetingType, sd.currency, sd.region UNION SELECT \'Sponsored Brands\' CAMPAIGN_TYPE ,sb.PROFILEID ,Date ,sb.campaignId as CampaignID ,sb.campaignName as CampaignName ,sb.adGroupId as AdGroupId ,sb.adGroupName as AdGroupName ,sb.keywordText as Keywordtext ,sb.campaignStatus as CampaignStatus ,sb.asin as Asin ,sb.adId as AdId ,sb.targetingExpression as TargetingExpression ,sb.targetingText as TargetingText ,sb.currency as Currency ,sb.region ,sum(sb.attributedOrdersNewToBrand14d) as NewToBrandOrders ,sum(sb.attributedSalesNewToBrand14d) as NewToBrandSales ,sum(sb.attributedUnitsOrderedNewToBrand14d) as NewToBrandUnits ,sum(sb.impressions) as Impressions ,sum(sb.clicks) as Clicks ,sum(sb.cost) as Spend ,sum(sb.attributedSales14d) as AdSales ,sum(sb.attributedConversions14d) as Conversions ,sum(sb.attributedConversions14dSameSKU) as ConversionsSameSKU ,sum(sb.attributedSales14dSameSKU) as SalesSameSKU ,sum(sb.attributedSales14dOtherSKU) as OtherSKUSales ,(sum(sb.attributedConversions14d) - sum(sb.attributedConversions14dSameSKU)) as ConversionsOtherSKU FROM assembly_db.MAPLEMONK.assembly_db_AA_Sponsored_Brands_Intermediate sb WHERE sb.RECORDTYPE = \'keywords\' and not (ifnull(cost,0) = 0 and ifnull(Impressions,0) = 0 and ifnull(Clicks,0) = 0 and ifnull(attributedSales14d,0) = 0 and ifnull(attributedConversions14d,0) = 0 and ifnull(attributedConversions14dSameSKU,0) = 0 and ifnull(attributedSales14dSameSKU,0) = 0 and ifnull(attributedSales14dOtherSKU,0) = 0 and ifnull(attributedConversions14d,0) = 0 and ifnull(attributedOrdersNewToBrand14d,0) = 0 and ifnull(attributedSalesNewToBrand14d,0) = 0 and ifnull(attributedUnitsOrderedNewToBrand14d,0) = 0) GROUP BY sb.DATE, sb.PROFILEID, sb.campaignId, sb.campaignName, sb.adGroupId, sb.adGroupName, sb.keywordText, sb.campaignStatus, sb.ASIN ,sb.adId, sb.targetingExpression, sb.targetingText, sb.targetingType, sb.currency, sb.region) select a.date as date ,a.asin as asin ,a.CAMPAIGN_TYPE ,a.PROFILEID ,a.CampaignID ,a.CampaignName ,a.AdGroupId ,a.AdGroupName ,a.KeywordText ,a.CampaignStatus ,a.AdId ,a.TargetingExpression ,a.TargetingText ,a.Currency ,a.NewToBrandOrders ,a.NewToBrandSales ,a.NewToBrandUnits ,a.Impressions ,a.Clicks ,a.spend ,a.AdSales ,a.Conversions ,a.ConversionsSameSKU ,a.SalesSameSKU ,a.OtherSKUSales ,a.ConversionsOtherSKU ,a.region from amazonAds a ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from assembly_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        