{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table vahdam_db.maplemonk.AA_IN_NEW_Sponsored_Products_Intermediate as select primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,campaignId ,campaignName ,adGroupId ,adGroupName ,campaignStatus ,keywordId ,keywordText ,adId ,matchType ,targetId ,targetingType ,targetingExpression ,targetingText ,currency ,asin ,otherAsin ,sku ,impressions ,clicks ,cost ,attributedSales14d ,attributedConversions14d ,attributedSales7d ,attributedConversions7d ,attributedConversions14dSameSKU ,attributedSales14dSameSKU ,attributedSales14dOtherSKU from ( select *,row_number()over(partition by primary_key order by _AIRBYTE_EMITTED_AT desc) as rw2 from (select array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int ,metric:campaignStatus::varchar,metric:keywordId::int,metric:matchType::varchar ,metric:targetId::int,metric:targetingType::varchar,metric:asin,metric:otherAsin::varchar,metric:sku::varchar)),\'\') primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,metric:campaignId::int as campaignId ,metric:campaignName::varchar as campaignName ,metric:adGroupId::int adGroupId ,metric:adGroupName::varchar as adGroupName ,metric:campaignStatus::varchar campaignStatus ,metric:keywordId::int keywordId ,metric:keywordText::varchar keywordText ,metric:adId::int adId ,metric:matchType::varchar matchType ,metric:targetId::int targetId ,metric:targetingType::varchar targetingType ,metric:targetingExpression::varchar targetingExpression ,metric:targetingText::varchar targetingText ,metric:currency::varchar currency ,metric:asin::varchar asin ,metric:otherAsin::varchar otherAsin ,metric:sku::varchar sku ,metric:impressions::float impressions ,metric:clicks::float clicks ,metric:cost::float cost ,metric:attributedSales14d::float attributedSales14d ,metric:attributedConversions14d::float attributedConversions14d ,metric:attributedSales7d::float attributedSales7d ,metric:attributedConversions7d::float attributedConversions7d ,metric:attributedConversions14dSameSKU::float attributedConversions14dSameSKU ,metric:attributedSales14dSameSKU::float attributedSales14dSameSKU ,metric:attributedSales14dOtherSKU::float attributedSales14dOtherSKU ,metric:impressions::float +metric:clicks::float +metric:cost::float +metric:attributedSales14d::float +metric:attributedConversions14d::float+metric:attributedSales7d::float +metric:attributedConversions7d::float as tt ,_AIRBYTE_EMITTED_AT ,row_number()over(partition by array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int ,metric:campaignStatus::varchar,metric:keywordId::int,metric:matchType::varchar ,metric:targetId::int,metric:targetingType::varchar,metric:asin,metric:otherAsin::varchar,metric:sku::varchar)),\'\') order by metric:impressions::float +metric:clicks::float +metric:cost::float +metric:attributedSales14d::float +metric:attributedConversions14d::float+metric:attributedSales7d::float +metric:attributedConversions7d::float desc) as rw from VAHDAM_DB.MAPLEMONK.AA_IN_NEW_SPONSORED_PRODUCTS_REPORT_STREAM where RECORDTYPE = \'productAds\' ) a where rw =1 or tt>0 ) b where rw2 =1; create or replace table vahdam_db.maplemonk.AA_IN_NEW_Sponsored_Brands_Intermediate as select primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,campaignId ,campaignName ,adGroupId ,adGroupName ,campaignStatus ,campaignBudgetType ,adId ,keywordId ,keywordText ,keywordStatus ,matchType ,targetId ,targetingType ,targetingExpression ,targetingText ,currency ,asin ,otherAsin ,sku ,impressions ,clicks ,cost ,attributedSales14d ,attributedConversions14d ,attributedSales7d ,attributedConversions7d ,attributedOrdersNewToBrand14d ,attributedSalesNewToBrand14d ,attributedUnitsOrderedNewToBrand14d ,attributedConversions14dSameSKU ,attributedSales14dSameSKU ,attributedSales14dOtherSKU from ( select *,row_number()over(partition by primary_key order by _AIRBYTE_EMITTED_AT desc) as rw2 from (select array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int, metric:campaignBudgetType::varchar ,metric:keywordText::varchar, metric:matchType::varchar)),\'\') primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,metric:campaignId::int as campaignId ,metric:campaignName::varchar as campaignName ,metric:adGroupId::int adGroupId ,metric:adGroupName::varchar as adGroupName ,metric:campaignStatus::varchar campaignStatus ,metric:campaignBudgetType::varchar campaignBudgetType ,metric:adId::int adId ,metric:keywordText::varchar keywordText ,metric:keywordStatus::varchar keywordStatus ,metric:keywordId::int keywordId ,metric:matchType::varchar matchType ,metric:targetId::int targetId ,metric:targetingType::varchar targetingType ,metric:targetingExpression::varchar targetingExpression ,metric:targetingText::varchar targetingText ,metric:currency::varchar currency ,metric:asin::varchar asin ,metric:otherAsin::varchar otherAsin ,metric:sku::varchar sku ,metric:impressions::float impressions ,metric:clicks::float clicks ,metric:cost::float cost ,metric:attributedSales14d::float attributedSales14d ,metric:attributedConversions14d::float attributedConversions14d ,metric:attributedSales7d::float attributedSales7d ,metric:attributedConversions7d::float attributedConversions7d ,metric:attributedOrdersNewToBrand14d::float attributedOrdersNewToBrand14d ,metric:attributedSalesNewToBrand14d::float attributedSalesNewToBrand14d ,metric:attributedUnitsOrderedNewToBrand14d::float attributedUnitsOrderedNewToBrand14d ,metric:attributedConversions14dSameSKU::float attributedConversions14dSameSKU ,metric:attributedSales14dSameSKU::float attributedSales14dSameSKU ,metric:attributedSales14dOtherSKU::int attributedSales14dOtherSKU ,metric:impressions::float +metric:clicks::float +metric:cost::float +metric:attributedSales14d::float +metric:attributedConversions14d::float+ifnull(metric:attributedSales7d::float,0) +ifnull(metric:attributedConversions7d::float,0) as tt ,_AIRBYTE_EMITTED_AT ,row_number()over(partition by array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int, metric:campaignBudgetType::varchar ,metric:keywordText::varchar, metric:matchType::varchar)),\'\') order by metric:impressions::float +metric:clicks::float +metric:cost::float +metric:attributedSales14d::float +metric:attributedConversions14d::float+metric:attributedSales7d::float +metric:attributedConversions7d::float desc) as rw from VAHDAM_DB.MAPLEMONK.AAI_IN_NEW_SPONSORED_BRANDS_REPORT_STREAM where RECORDTYPE = \'keywords\' ) a where rw =1 or tt>0 ) b where rw2 =1; create or replace table vahdam_db.maplemonk.AA_IN_NEW_Sponsored_Brands_Video_Intermediate as select primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,campaignId ,campaignName ,adGroupId ,adGroupName ,campaignStatus ,campaignBudgetType ,adId ,keywordId ,keywordText ,keywordStatus ,matchType ,targetId ,targetingType ,targetingExpression ,targetingText ,currency ,asin ,otherAsin ,sku ,impressions ,clicks ,cost ,attributedSales14d ,attributedConversions14d ,attributedSales7d ,attributedConversions7d ,attributedConversions14dSameSKU ,attributedSales14dSameSKU ,attributedSales14dOtherSKU from ( select *,row_number()over(partition by primary_key order by _AIRBYTE_EMITTED_AT desc) as rw2 from (select array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int, metric:campaignBudgetType::varchar ,metric:keywordText::varchar, metric:matchType::varchar ,metric:targetingId::int, metric:targetingType::varchar)),\'\') primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,metric:campaignId::int as campaignId ,metric:campaignName::varchar as campaignName ,metric:adGroupId::int adGroupId ,metric:adGroupName::varchar as adGroupName ,metric:campaignStatus::varchar campaignStatus ,metric:campaignBudgetType::varchar campaignBudgetType ,metric:adId::int adId ,metric:keywordText::varchar keywordText ,metric:keywordStatus::varchar keywordStatus ,metric:keywordId::int keywordId ,metric:matchType::varchar matchType ,metric:targetId::int targetId ,metric:targetingType::varchar targetingType ,metric:targetingExpression::varchar targetingExpression ,metric:targetingText::varchar targetingText ,metric:currency::varchar currency ,metric:asin::varchar asin ,metric:otherAsin::varchar otherAsin ,metric:sku::varchar sku ,metric:impressions::float impressions ,metric:clicks::float clicks ,metric:cost::float cost ,metric:attributedSales14d::float attributedSales14d ,metric:attributedConversions14d::float attributedConversions14d ,metric:attributedSales7d::float attributedSales7d ,metric:attributedConversions7d::float attributedConversions7d ,metric:attributedConversions14dSameSKU::float attributedConversions14dSameSKU ,metric:attributedSales14dSameSKU::float attributedSales14dSameSKU ,metric:attributedSales14dOtherSKU::int attributedSales14dOtherSKU ,metric:impressions::float +metric:clicks::float +metric:cost::float +metric:attributedSales14d::float +metric:attributedConversions14d::float+ifnull(metric:attributedSales7d::float,0) +ifnull(metric:attributedConversions7d::float,0) as tt ,_AIRBYTE_EMITTED_AT ,row_number()over(partition by array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int, metric:campaignBudgetType::varchar ,metric:keywordText::varchar, metric:matchType::varchar ,metric:targetingId::int, metric:targetingType::varchar)),\'\') order by metric:impressions::float +metric:clicks::float +metric:cost::float +metric:attributedSales14d::float +metric:attributedConversions14d::float+metric:attributedSales7d::float +metric:attributedConversions7d::float desc) as rw from VAHDAM_DB.MAPLEMONK.AAI_IN_NEW_SPONSORED_BRANDS_VIDEO_REPORT_STREAM where RECORDTYPE = \'keywords\' ) a where rw =1 or tt>0 ) b where rw2 =1; create or replace table vahdam_db.maplemonk.AA_IN_NEW_Sponsored_Display_Intermediate as select primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,campaignId ,campaignName ,adGroupId ,adGroupName ,campaignStatus ,campaignBudgetType ,adId ,keywordId ,keywordText ,keywordStatus ,matchType ,targetId ,targetingType ,targetingExpression ,targetingText ,currency ,asin ,otherAsin ,sku ,impressions ,clicks ,cost ,attributedSales14d ,attributedConversions14d ,attributedSales7d ,attributedConversions7d ,attributedOrdersNewToBrand14d ,attributedSalesNewToBrand14d ,attributedUnitsOrderedNewToBrand14d ,attributedConversions14dSameSKU ,attributedSales14dSameSKU ,attributedSales14dOtherSKU from ( select *,row_number()over(partition by primary_key order by _AIRBYTE_EMITTED_AT desc) as rw2 from (select array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int, metric:adId::int, metric:targetId::int, metric:targetingType::varchar, metric:asin::varchar, metric:otherAsin::varchar, metric:sku::varchar)),\'\') primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,metric:campaignId::int as campaignId ,metric:campaignName::varchar as campaignName ,metric:adGroupId::int adGroupId ,metric:adGroupName::varchar as adGroupName ,metric:campaignStatus::varchar campaignStatus ,metric:campaignBudgetType::varchar campaignBudgetType ,metric:keywordText::varchar keywordText ,metric:keywordStatus::varchar keywordStatus ,metric:keywordId::int keywordId ,metric:matchType::varchar matchType ,metric:adId::int adId ,metric:targetId::int targetId ,metric:targetingType::varchar targetingType ,metric:targetingExpression::varchar targetingExpression ,metric:targetingText::varchar targetingText ,metric:currency::varchar currency ,metric:asin::varchar asin ,metric:otherAsin::varchar otherAsin ,metric:sku::varchar sku ,metric:impressions::float impressions ,metric:clicks::float clicks ,metric:cost::float cost ,metric:attributedSales14d::float attributedSales14d ,metric:attributedConversions14d::float attributedConversions14d ,metric:attributedSales7d::float attributedSales7d ,metric:attributedConversions7d::float attributedConversions7d ,metric:attributedOrdersNewToBrand14d::float attributedOrdersNewToBrand14d ,metric:attributedSalesNewToBrand14d::float attributedSalesNewToBrand14d ,metric:attributedUnitsOrderedNewToBrand14d::float attributedUnitsOrderedNewToBrand14d ,metric:attributedConversions14dSameSKU::float attributedConversions14dSameSKU ,metric:attributedSales14dSameSKU::float attributedSales14dSameSKU ,metric:attributedSales14dOtherSKU::float attributedSales14dOtherSKU ,metric:impressions::float +metric:clicks::float +metric:cost::float +metric:attributedSales14d::float +metric:attributedConversions14d::float+metric:attributedSales7d::float +metric:attributedConversions7d::float as tt ,_AIRBYTE_EMITTED_AT ,row_number()over(partition by array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int, metric:adId::int, metric:targetId::int, metric:targetingType::varchar, metric:asin::varchar, metric:otherAsin::varchar, metric:sku::varchar)),\'\') order by metric:impressions::float +metric:clicks::float +metric:cost::float +metric:attributedSales14d::float +metric:attributedConversions14d::float+metric:attributedSales7d::float +metric:attributedConversions7d::float desc) as rw from VAHDAM_DB.MAPLEMONK.AA_IN_NEW_SPONSORED_DISPLAY_REPORT_STREAM where RECORDTYPE = \'productAds\' ) a where rw =1 or tt>0 ) b where rw2 =1; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.AMAZONADS_IN_MARKETING_NEW AS with orders as ( select \"purchase-date\"::date as order_date, asin, sum(ifnull(try_cast(\"item-price\" as float),0) - ifnull(try_cast(\"item-promotion-discount\" as float),0)) as sales, count(distinct \"amazon-order-id\") as orders, sum(Quantity) as quantity from \"VAHDAM_DB\".\"MAPLEMONK\".\"ASP_IN_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL\" where \"sales-channel\" = \'Amazon.in\' group by 1,2 ), amazon as ( SELECT \'Sponsored Products\' CAMPAIGN_TYPE, sp.PROFILEID ,TO_DATE(sp.REPORTDATE,\'YYYYMMDD\') AS Date ,sp.CampaignId ,sp.campaignName ,sp.adGroupId ,sp.adGroupName ,sp.keywordText ,sp.campaignStatus ,sp.ASIN as Asin ,sp.adId ,sp.targetingExpression ,sp.targetingText ,sp.currency ,Null as NewToBrandOrders ,Null as NewToBrandSales ,Null as NewToBrandUnits ,sum(sp.impressions) as Impressions ,sum(sp.clicks) as Clicks ,sum(sp.cost) as Spend ,sum(sp.attributedSales7d) as Sales ,sum(sp.attributedConversions7d) as Conversions ,sum(sp.attributedConversions14dSameSKU) as ConversionsSameSKU ,sum(sp.attributedSales14dSameSKU) as SalesSameSKU ,sum(sp.attributedSales14dOtherSKU) as OtherSKUSales ,(sum(sp.attributedConversions14d) - sum(sp.attributedConversions14dSameSKU)) as ConversionsOtherSKU FROM vahdam_db.maplemonk.AA_IN_NEW_Sponsored_Products_Intermediate sp WHERE sp.RECORDTYPE = \'productAds\' and sp.campaignName not in (\'Auto - Turmeric | MR\', \'Auto - Advertising Ready | MR\', \'Manual - Matcha_KW Campaign Aug - 21 | MR\', \'Manual - World Tea Private Reserve B07RBQXDQK | MR\', \'Auto - Low Sessions | MR\' , \'Manual - B00VIDY1GC BLUSH - 3 Loose Leaf Teas (3 Tin Caddy) | MR\', \'Diwali | BLUSH - 3 Loose Leaf Teas (3 Tin Caddy) B00VIDY1GC\' , \'Diwali | BLOOM (9 Tin Caddy) B07RBN3ZMJ\', \'Dilwai | GLOW - 6 Loose Leaf Teas (6 Tin Caddy) B015J3FXOU\' ,\'Diwali | Founder\\'s Select - 8 x 5 TB B01KCBXLH0\', \'Diwali | World Tea Private Reserve (6 Tin Caddy) B07RBQXDQK\') GROUP BY sp.REPORTDATE, sp.PROFILEID, sp.campaignId, sp.campaignName, sp.adGroupId, sp.adGroupName, sp.keywordText ,sp.campaignStatus, sp.ASIN, sp.adId, sp.targetingExpression, sp.targetingText, sp.targetingType, sp.currency UNION SELECT \'Sponsored Display\' CAMPAIGN_TYPE, sd.PROFILEID ,TO_DATE(sd.REPORTDATE,\'YYYYMMDD\') AS Date ,sd.campaignId as CampaignID ,sd.campaignName as CampaignName ,sd.adGroupId as AdGroupId ,sd.adGroupName as AdGroupName ,sd.keywordText as Keywordtext ,sd.campaignStatus as CampaignStatus ,sd.asin as Asin ,sd.adId as AdId ,sd.targetingExpression as TargetingExpression ,sd.targetingText as TargetingText ,sd.currency as Currency ,sum(sd.attributedOrdersNewToBrand14d) as NewToBrandOrders ,sum(sd.attributedSalesNewToBrand14d) as NewToBrandSales ,sum(sd.attributedUnitsOrderedNewToBrand14d) as NewToBrandUnits ,sum(sd.impressions) as Impressions ,sum(sd.clicks) as Clicks ,sum(sd.cost) as Spend ,sum(sd.attributedSales7d) as Sales ,sum(sd.attributedConversions7d) as Conversions ,sum(sd.attributedConversions14dSameSKU) as ConversionsSameSKU ,sum(sd.attributedSales14dSameSKU) as SalesSameSKU ,sum(sd.attributedSales14dOtherSKU) as OtherSKUSales ,(sum(sd.attributedConversions14d) - sum(sd.attributedConversions14dSameSKU)) as ConversionsOtherSKU FROM vahdam_db.maplemonk.AA_IN_NEW_Sponsored_Display_Intermediate sd WHERE sd.RECORDTYPE = \'productAds\' GROUP BY sd.REPORTDATE, sd.PROFILEID, sd.campaignId, sd.campaignName, sd.adGroupId, sd.adGroupName, sd.keywordText, sd.campaignStatus ,sd.ASIN, sd.adId ,sd.targetingExpression, sd.targetingText, sd.targetingType, sd.currency UNION SELECT \'Sponsored Brands\' CAMPAIGN_TYPE, sb.PROFILEID ,TO_DATE(sb.REPORTDATE,\'YYYYMMDD\') AS Date ,sb.campaignId as CampaignID ,sb.campaignName as CampaignName ,sb.adGroupId as AdGroupId ,sb.adGroupName as AdGroupName ,sb.keywordText as Keywordtext ,sb.campaignStatus as CampaignStatus ,sb.asin as Asin ,sb.adId as AdId ,sb.targetingExpression as TargetingExpression ,sb.targetingText as TargetingText ,sb.currency as Currency ,sum(sb.attributedOrdersNewToBrand14d) as NewToBrandOrders ,sum(sb.attributedSalesNewToBrand14d) as NewToBrandSales ,sum(sb.attributedUnitsOrderedNewToBrand14d) as NewToBrandUnits ,sum(sb.impressions) as Impressions ,sum(sb.clicks) as Clicks ,sum(sb.cost) as Spend ,sum(sb.attributedSales14d) as Sales ,sum(sb.attributedConversions14d) as Conversions ,sum(sb.attributedConversions14dSameSKU) as ConversionsSameSKU ,sum(sb.attributedSales14dSameSKU) as SalesSameSKU ,sum(sb.attributedSales14dOtherSKU) as SalesOtherSKU ,(sum(sb.attributedConversions14d) - sum(sb.attributedConversions14dSameSKU)) as ConversionsOtherSKU FROM vahdam_db.maplemonk.AA_IN_NEW_Sponsored_Brands_Intermediate sb WHERE sb.RECORDTYPE = \'keywords\' GROUP BY sb.REPORTDATE, sb.PROFILEID, sb.campaignId, sb.campaignName, sb.adGroupId, sb.adGroupName, sb.keywordText ,sb.campaignStatus, sb.ASIN, sb.adId, sb.targetingExpression, sb.targetingText, sb.targetingType, sb.currency UNION SELECT \'Sponsored Brands Video\' CAMPAIGN_TYPE, sbv.PROFILEID ,TO_DATE(sbv.REPORTDATE,\'YYYYMMDD\') AS Date ,sbv.campaignId as CampaignID ,sbv.campaignName as CampaignName ,sbv.adGroupId as AdGroupId ,sbv.adGroupName as AdGroupName ,sbv.keywordText as Keywordtext ,sbv.campaignStatus as CampaignStatus ,sbv.asin as Asin ,sbv.adId as AdId ,sbv.targetingExpression as TargetingExpression ,sbv.targetingText as TargetingText ,sbv.currency as Currency ,Null as NewToBrandOrders ,Null as NewToBrandSales ,Null as NewToBrandUnits ,sum(sbv.impressions) as Impressions ,sum(sbv.clicks) as Clicks ,sum(sbv.cost) as Spend ,sum(sbv.attributedSales14d) as Sales ,sum(sbv.attributedConversions14d) as Conversions ,sum(sbv.attributedConversions14dSameSKU) as ConversionsSameSKU ,sum(sbv.attributedSales14dSameSKU) as SalesSameSKU ,sum(sbv.attributedSales14dOtherSKU) as SalesOtherSKU ,(sum(sbv.attributedConversions14d) - sum(sbv.attributedConversions14dSameSKU)) as ConversionsOtherSKU FROM vahdam_db.maplemonk.AA_IN_NEW_Sponsored_Brands_Video_Intermediate sbv WHERE sbv.RECORDTYPE = \'keywords\' GROUP BY sbv.REPORTDATE, sbv.PROFILEID, sbv.campaignId, sbv.campaignName, sbv.adGroupId, sbv.adGroupName , sbv.keywordText, sbv.campaignStatus, sbv.asin, sbv.adId, sbv.targetingExpression, sbv.targetingText , sbv.targetingType, sbv.currency ) select a.*,o.Sales/count(1)over(partition by a.asin,a.date) as Sales_usd, o.orders/count(1)over(partition by a.asin,a.date) as orders, o.quantity/count(1)over(partition by a.asin,a.date) as quantity from amazon a left join orders o on a.asin = o.asin and order_date::date = a.Date::date where a.Date::date > \'2022-05-29\'; create or replace table VAHDAM_DB.MAPLEMONK.AMAZONADS_IN_MARKETING as select * from VAHDAM_DB.MAPLEMONK.AMAZONADS_IN_MARKETING_OLD union all select * from VAHDAM_DB.MAPLEMONK.AMAZONADS_IN_MARKETING_NEW;",
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
                        