{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table select_db.maplemonk.select_db_amazonads_fact_items_sp_targets as select primary_key ,TO_DATE(REPORTDATE,\'YYYY-MM-DD\') AS Date ,RECORDTYPE ,PROFILEID ,b.campaignId ,campaignName ,adGroupId ,adGroupName ,campaignStatus ,keywordId ,keywordText ,adId ,matchType ,targetId ,b.targetingType ,targetingExpression ,targetingText ,currency ,asin ,otherAsin ,sku ,impressions ,clicks ,cost ,attributedSales14d ,attributedConversions14d ,attributedSales7d ,attributedConversions7d ,attributedConversions14dSameSKU ,attributedSales14dSameSKU ,attributedSales14dOtherSKU ,\'EU\' as region ,campaign_data.state ,campaign_data.targetingtype CampaignTargetingType ,campaign_data.percentage campaign_placement_bidding_percentage ,campaign_data.placement campaign_placement_bidding ,campaign_data.strategy campaign_strategy ,campaign_data.Campaign_budget ,campaign_data.campaign_budget_type ,upper(case when lower(b.targetingtype) like \'%asin%\' then \'Product\' when lower(b.targetingtype) like \'%category%\' then \'Product Category\' when lower(b.targetingtype) is null then \'Keyword\' else b.targetingtype end) Final_Ad_Category from ( select * ,row_number()over(partition by primary_key order by _AIRBYTE_EMITTED_AT desc) as rw2 from (select array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int,metric:adId::int,metric:campaignStatus::varchar,metric:keywordId::int,metric:matchType::varchar,metric:targetId::int,metric:targetingType::varchar,metric:advertisedAsin::varchar,metric:purchasedAsin::varchar,metric:advertisedSku::varchar)),\'\') primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,metric:campaignId::int as campaignId ,metric:campaignName::varchar as campaignName ,metric:adGroupId::int adGroupId ,metric:adGroupName::varchar as adGroupName ,metric:campaignStatus::varchar campaignStatus ,metric:keywordId::int keywordId ,metric:keyword::varchar keywordText ,metric:adId::int adId ,metric:matchType::varchar matchType ,NULL targetId ,metric:targeting::varchar targetingType ,NULL targetingExpression ,NULL targetingText ,\'INR\' currency ,metric:advertisedAsin::varchar asin ,metric:purchasedAsin::varchar otherAsin ,metric:advertisedSku::varchar sku ,metric:impressions::float impressions ,metric:clicks::float clicks ,metric:cost::float cost ,metric:sales14d::float attributedSales14d ,metric:purchases14d::float attributedConversions14d ,metric:sales7d::float attributedSales7d ,metric:purchases7d::float attributedConversions7d ,metric:purchasesSameSku14d::float attributedConversions14dSameSKU ,metric:attributedSalesSameSku14d::float attributedSales14dSameSKU ,metric:salesOtherSku14d::float attributedSales14dOtherSKU ,IFNULL(metric:impressions::float,0)+ IFNULL(metric:clicks::float,0)+ IFNULL(metric:cost::float,0) + IFNULL(metric:sales14d::float,0) + IFNULL(metric:purchases14d::float,0)+ IFNULL(metric:sales7d::float,0) + IFNULL(metric:purchases7d::float,0) as tt ,_AIRBYTE_EMITTED_AT ,row_number() over (partition by array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int,metric:adId::int ,metric:campaignStatus::varchar,metric:keywordId::int,metric:matchType::varchar,metric:advertisedAsin,metric:purchasedAsin::varchar,metric:advertisedSku::varchar)),\'\') order by IFNULL(metric:impressions::float,0)+ IFNULL(metric:clicks::float,0)+ IFNULL(metric:cost::float,0) + IFNULL(metric:sales14d::float,0) + IFNULL(metric:purchases14d::float,0)+ IFNULL(metric:sales7d::float,0) + IFNULL(metric:purchases7d::float,0) desc) as rw from SELECT_DB.MAPLEMONK.Amazon_Advertisement_AA_SELECT_KYARI_SPONSORED_PRODUCTS_REPORT_STREAM where lower(RECORDTYPE) = \'targets\' ) a where rw =1 or tt>0 ) b left join (select * from (select campaignid ,state ,targetingtype ,PARSE_JSON(dynamicbidding):placementBidding[0]:percentage::INTEGER AS percentage ,PARSE_JSON(dynamicbidding):placementBidding[0]:placement::STRING AS placement ,PARSE_JSON(dynamicbidding):strategy::STRING AS strategy ,budget:\"budget\"::integer as Campaign_budget ,replace(budget:\"budgetType\",\'\"\',\'\') as campaign_budget_type ,row_number() over (partition by campaignid order by _airbyte_normalized_at desc) rw from select_db.maplemonk.amazon_advertisement_aa_select_kyari_sponsored_product_campaigns_v3_mm ) where rw=1) campaign_data on b.campaignid = campaign_data.campaignid where b.rw2 =1 ; create or replace table select_db.maplemonk.select_db_amazonads_fact_items_sp_keywords as select primary_key ,TO_DATE(REPORTDATE,\'YYYY-MM-DD\') AS Date ,RECORDTYPE ,PROFILEID ,b.campaignId ,campaignName ,adGroupId ,adGroupName ,campaignStatus ,keywordId ,keywordText ,adId ,matchType ,targetId ,b.targetingType ,targetingExpression ,targetingText ,currency ,asin ,otherAsin ,sku ,impressions ,clicks ,cost ,attributedSales14d ,attributedConversions14d ,attributedSales7d ,attributedConversions7d ,attributedConversions14dSameSKU ,attributedSales14dSameSKU ,attributedSales14dOtherSKU ,\'EU\' as region ,campaign_data.state ,campaign_data.targetingtype CampaignTargetingType ,campaign_data.percentage campaign_placement_bidding_percentage ,campaign_data.placement campaign_placement_bidding ,campaign_data.strategy campaign_strategy ,campaign_data.Campaign_budget ,campaign_data.campaign_budget_type ,upper(case when lower(b.targetingtype) like \'%asin%\' then \'Product\' when lower(b.targetingtype) like \'%category%\' then \'Product Category\' when lower(b.targetingtype) is null then \'Keyword\' else b.targetingtype end) Final_Ad_Category from ( select * ,row_number()over(partition by primary_key order by _AIRBYTE_EMITTED_AT desc) as rw2 from (select array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int,metric:adId::int,metric:campaignStatus::varchar,metric:keywordId::int,metric:matchType::varchar,metric:targetId::int,metric:targetingType::varchar,metric:advertisedAsin::varchar,metric:purchasedAsin::varchar,metric:advertisedSku::varchar)),\'\') primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,metric:campaignId::int as campaignId ,metric:campaignName::varchar as campaignName ,metric:adGroupId::int adGroupId ,metric:adGroupName::varchar as adGroupName ,metric:campaignStatus::varchar campaignStatus ,metric:keywordId::int keywordId ,metric:keyword::varchar keywordText ,metric:adId::int adId ,metric:matchType::varchar matchType ,NULL targetId ,metric:targeting::varchar targetingType ,NULL targetingExpression ,NULL targetingText ,\'INR\' currency ,metric:advertisedAsin::varchar asin ,metric:purchasedAsin::varchar otherAsin ,metric:advertisedSku::varchar sku ,metric:impressions::float impressions ,metric:clicks::float clicks ,metric:cost::float cost ,metric:sales14d::float attributedSales14d ,metric:purchases14d::float attributedConversions14d ,metric:sales7d::float attributedSales7d ,metric:purchases7d::float attributedConversions7d ,metric:purchasesSameSku14d::float attributedConversions14dSameSKU ,metric:attributedSalesSameSku14d::float attributedSales14dSameSKU ,metric:salesOtherSku14d::float attributedSales14dOtherSKU ,IFNULL(metric:impressions::float,0)+ IFNULL(metric:clicks::float,0)+ IFNULL(metric:cost::float,0) + IFNULL(metric:sales14d::float,0) + IFNULL(metric:purchases14d::float,0)+ IFNULL(metric:sales7d::float,0) + IFNULL(metric:purchases7d::float,0) as tt ,_AIRBYTE_EMITTED_AT ,row_number() over (partition by array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int,metric:adId::int ,metric:campaignStatus::varchar,metric:keywordId::int,metric:matchType::varchar,metric:advertisedAsin,metric:purchasedAsin::varchar,metric:advertisedSku::varchar)),\'\') order by IFNULL(metric:impressions::float,0)+ IFNULL(metric:clicks::float,0)+ IFNULL(metric:cost::float,0) + IFNULL(metric:sales14d::float,0) + IFNULL(metric:purchases14d::float,0)+ IFNULL(metric:sales7d::float,0) + IFNULL(metric:purchases7d::float,0) desc) as rw from SELECT_DB.MAPLEMONK.Amazon_Advertisement_AA_SELECT_KYARI_SPONSORED_PRODUCTS_REPORT_STREAM where lower(RECORDTYPE) = \'keywords\' ) a where rw =1 or tt>0 ) b left join (select * from (select campaignid ,state ,targetingtype ,PARSE_JSON(dynamicbidding):placementBidding[0]:percentage::INTEGER AS percentage ,PARSE_JSON(dynamicbidding):placementBidding[0]:placement::STRING AS placement ,PARSE_JSON(dynamicbidding):strategy::STRING AS strategy ,budget:\"budget\"::integer as Campaign_budget ,replace(budget:\"budgetType\",\'\"\',\'\') as campaign_budget_type ,row_number() over (partition by campaignid order by _airbyte_normalized_at desc) rw from select_db.maplemonk.amazon_advertisement_aa_select_kyari_sponsored_product_campaigns_v3_mm ) where rw=1) campaign_data on b.campaignid = campaign_data.campaignid where rw2 =1; create or replace table SELECT_DB.MAPLEMONK.select_db_amazonads_fact_items_sd_targets as select primary_key ,TO_DATE(REPORTDATE,\'YYYYMMDD\') AS Date ,RECORDTYPE ,PROFILEID ,b.campaignId ,b.campaignName ,b.adGroupId ,adGroupName ,campaignStatus ,b.adId ,b.keywordId ,keywordText ,matchType ,b.targetId ,b.targetingType ,b.targetingExpression ,b.targetingText ,currency ,asin ,otherAsin ,sku ,impressions ,clicks ,cost ,attributedSales14d ,attributedConversions14d ,attributedSales7d ,attributedConversions7d ,attributedConversions14dSameSKU ,attributedSales14dSameSKU ,attributedSales14dOtherSKU ,\'EU\' as region ,campaign_data.state ,target_data.targetingtype CAMPAIGNTARGETINGTYPE ,NULL as CAMPAIGN_PLACEMENT_BIDDING_PERCENTAGE ,campaign_data.costtype as CAMPAIGN_PLACEMENT_BIDDING ,NULL as CAMPAIGN_STRATEGY ,campaign_data.Campaign_budget ,campaign_data.campaign_budget_type ,target_data.placement as FINAL_AD_CATEGORY from ( select * ,row_number()over(partition by primary_key order by _AIRBYTE_EMITTED_AT desc) as rw2 from (select array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int,metric:adId::int, metric:targetId::int, metric:targetingType::varchar, metric:asin::varchar, metric:otherAsin::varchar, metric:sku::varchar)),\'\') primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,metric:campaignId::int as campaignId ,metric:campaignName::varchar as campaignName ,metric:adGroupId::int adGroupId ,metric:adGroupName::varchar as adGroupName ,metric:campaignStatus::varchar campaignStatus ,NULL campaignBudgetType ,NULL keywordText ,NULL keywordStatus ,NULL keywordId ,NULL matchType ,metric:adId::int adId ,metric:targetId::int targetId ,metric:targetingType::varchar targetingType ,metric:targetingExpression::varchar targetingExpression ,metric:targetingText::varchar targetingText ,\'INR\' currency ,metric:asin::varchar asin ,metric:otherAsin::varchar otherAsin ,metric:sku::varchar sku ,metric:impressions::float impressions ,metric:clicks::float clicks ,metric:cost::float cost ,metric:attributedSales14d::float attributedSales14d ,metric:attributedConversions14d::float attributedConversions14d ,metric:attributedSales7d::float attributedSales7d ,metric:attributedConversions7d::float attributedConversions7d ,metric:attributedOrdersNewToBrand14d::float attributedOrdersNewToBrand14d ,metric:attributedSalesNewToBrand14d::float attributedSalesNewToBrand14d ,metric:attributedUnitsOrderedNewToBrand14d::float attributedUnitsOrderedNewToBrand14d ,metric:attributedConversions14dSameSKU::float attributedConversions14dSameSKU ,metric:attributedSales14dSameSKU::float attributedSales14dSameSKU ,metric:attributedSales14dOtherSKU::float attributedSales14dOtherSKU ,IFNULL(metric:impressions::float,0) + IFNULL(metric:clicks::float,0) + IFNULL(metric:cost::float,0) + IFNULL(metric:attributedSales14d::float,0) + IFNULL(metric:attributedConversions14d::float,0) + IFNULL(metric:attributedSales7d::float,0) + IFNULL(metric:attributedConversions7d::float,0) as tt ,_AIRBYTE_EMITTED_AT ,row_number()over(partition by array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int,metric:adId::int,metric:targetId::int, metric:targetingType::varchar, metric:asin::varchar, metric:otherAsin::varchar, metric:sku::varchar)),\'\') order by IFNULL(metric:impressions::float,0) + IFNULL(metric:clicks::float,0) + IFNULL(metric:cost::float,0) + IFNULL(metric:attributedSales14d::float,0) + IFNULL(metric:attributedConversions14d::float,0) + IFNULL(metric:attributedSales7d::float,0) + IFNULL(metric:attributedConversions7d::float,0) desc) as rw from SELECT_DB.MAPLEMONK.Amazon_Advertisement_AA_SELECT_KYARI_SPONSORED_DISPLAY_REPORT_STREAM where RECORDTYPE = \'targets\' ) a where rw =1 or tt>0 ) b left join (select * from (select campaignid ,adgroupid ,targetid ,upper(replace(EXPRESSION[0]:\"type\",\'\"\',\'\')) placement ,upper(expressiontype) expressiontype ,case when lower(placement) like any (\'%views%\',\'%purchases%\', \'%audience%\') then \'AUDIENCE\' else \'CONTEXTUAL\' end targetingtype ,row_number() over (partition by targetid order by _airbyte_normalized_at desc) rw from select_db.maplemonk.amazon_advertisement_aa_select_kyari_sponsored_display_targetings ) where rw=1 ) target_data on b.targetId = target_data.targetid left join (select * from (select CAMPAIGNID ,upper(costtype) costtype ,Budget Campaign_budget ,upper(BUDGETTYPE) campaign_budget_type ,upper(state) state ,row_number() over (partition by campaignid order by _airbyte_normalized_at desc) rw from select_db.maplemonk.amazon_advertisement_aa_select_kyari_sponsored_display_campaigns ) where rw = 1 ) campaign_data on b.campaignid = campaign_data.CAMPAIGNID where b.rw2 =1; create or replace table SELECT_DB.MAPLEMONK.select_db_amazonads_fact_items_sb_ad as select primary_key ,TO_DATE(REPORTDATE,\'YYYY-MM-DD\') AS Date ,RECORDTYPE ,PROFILEID ,b.campaignId ,b.campaignName ,b.adGroupId ,b.adGroupName ,campaignStatus ,b.adId ,keywordId ,keywordText ,keywordStatus ,matchType ,targetId ,b.targetingType ,b.targetingExpression ,b.targetingText ,currency ,asin ,otherAsin ,sku ,impressions ,clicks ,cost ,attributedSales14d ,attributedConversions14d ,attributedSales7d ,attributedConversions7d ,attributedOrdersNewToBrand14d ,attributedSalesNewToBrand14d ,attributedUnitsOrderedNewToBrand14d ,attributedConversions14dSameSKU ,attributedSales14dSameSKU ,attributedSales14dOtherSKU ,\'EU\' as region ,ad_data.STATE ,ad_data.targetingtype CAMPAIGNTARGETINGTYPE ,null CAMPAIGN_PLACEMENT_BIDDING_PERCENTAGE ,ad_data.PLACEMENT CAMPAIGN_PLACEMENT_BIDDING ,null CAMPAIGN_STRATEGY ,null CAMPAIGN_BUDGET ,b.campaignBudgetType CAMPAIGN_BUDGET_TYPE from ( select * ,row_number()over(partition by primary_key order by _AIRBYTE_EMITTED_AT desc) as rw2 from (select array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int,metric:campaignBudgetType::varchar,metric:keywordId::int,metric:keywordText::varchar, metric:matchType::varchar)),\'\') primary_key ,REPORTDATE ,RECORDTYPE ,PROFILEID ,metric:campaignId::int as campaignId ,metric:campaignName::varchar as campaignName ,metric:adGroupId::int adGroupId ,metric:adGroupName::varchar as adGroupName ,metric:campaignStatus::varchar campaignStatus ,metric:campaignBudgetType::varchar campaignBudgetType ,metric:adId adID ,metric:keywordText::varchar keywordText ,metric:keywordStatus::varchar keywordStatus ,metric:keywordId::int keywordId ,metric:matchType::varchar matchType ,metric:targetingId targetId ,metric:targetingType targetingType ,metric:targetingExpression targetingExpression ,metric:targetingText targetingText ,\'INR\' currency ,metric:purchasedAsin asin ,NULL otherAsin ,NULL sku ,metric:impressions::float impressions ,metric:clicks::float clicks ,metric:cost::float cost ,metric:sales::float attributedSales14d ,metric:purchases::float attributedConversions14d ,NULL attributedSales7d ,NULL attributedConversions7d ,metric:newToBrandPurchases14d::float attributedOrdersNewToBrand14d ,metric:newToBrandSales14d::float attributedSalesNewToBrand14d ,metric:newToBrandUnitsSold14d::float attributedUnitsOrderedNewToBrand14d ,null attributedConversions14dSameSKU ,null attributedSales14dSameSKU ,null attributedSales14dOtherSKU ,IFNULL(metric:impressions::float,0) +IFNULL(metric:clicks::float,0) +IFNULL(metric:cost::float,0) +IFNULL(metric:sales::float,0) +IFNULL(metric:purchases::float,0) as tt ,_AIRBYTE_EMITTED_AT ,row_number() over (partition by array_to_string(array_compact(array_construct( REPORTDATE,RECORDTYPE,PROFILEID,metric:campaignId::int,metric:adGroupId::int,metric:campaignBudgetType::varchar,metric:keywordId::int,metric:keywordText::varchar, metric:matchType::varchar)),\'\') order by IFNULL(metric:impressions::float,0) + IFNULL(metric:clicks::float,0) + IFNULL(metric:cost::float,0) + IFNULL(metric:sales::float,0) + IFNULL(metric:purchases::float,0) desc) as rw from SELECT_DB.MAPLEMONK.Amazon_Advertisement_AA_SELECT_KYARI_SPONSORED_BRANDS_REPORT_STREAM_V3_MM where RECORDTYPE = \'ad\' ) a where rw =1 or tt>0 ) b left join (select * from (select ADID ,ADGROUPID ,CAMPAIGNID ,STATE ,Upper(replace(PARSE_JSON(landingpage):\"pageType\",\'\"\',\'\')) PLACEMENT ,Upper(replace(PARSE_JSON(CREATIVE):\"type\",\'\"\',\'\')) targetingtype ,row_number() over (partition by ADID order by _airbyte_emitted_at desc) rw from select_db.maplemonk.amazon_advertisement_aa_select_kyari_sponsored_brands_ads_v4_mm ) where rw = 1 ) ad_data on b.adid = ad_Data.adid where rw2 =1; create or replace table select_db.maplemonk.select_db_amazon_ads_fact_items_w_dimensions as select b.profile_name ACCOUNT_NAME ,\'SPONSORED PRODUCTS\' CAMPAIGN_TYPE ,PRIMARY_KEY ,DATE ,RECORDTYPE ,b.PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,CAMPAIGNSTATUS ,KEYWORDID ,KEYWORDTEXT ,ADID ,MATCHTYPE ,TARGETID ,TARGETINGTYPE ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,ASIN ,OTHERASIN ,SKU ,IMPRESSIONS ,CLICKS ,COST ,ATTRIBUTEDSALES14D ,ATTRIBUTEDCONVERSIONS14D ,ATTRIBUTEDSALES7D ,ATTRIBUTEDCONVERSIONS7D ,ATTRIBUTEDCONVERSIONS14DSAMESKU ,ATTRIBUTEDSALES14DSAMESKU ,ATTRIBUTEDSALES14DOTHERSKU ,REGION ,STATE ,CAMPAIGNTARGETINGTYPE ,CAMPAIGN_PLACEMENT_BIDDING_PERCENTAGE ,CAMPAIGN_PLACEMENT_BIDDING ,CAMPAIGN_STRATEGY ,CAMPAIGN_BUDGET ,CAMPAIGN_BUDGET_TYPE ,FINAL_AD_CATEGORY from select_db.maplemonk.select_db_amazonads_fact_items_sp_targets a left join ( select profileid , upper(concat(\'AMAZON_\',replace(accountinfo:name,\'\"\',\'\'))) profile_name from select_db.maplemonk.amazon_advertisement_aa_select_kyari_profiles ) b on a.PROFILEID = b.profileid union all select b.profile_name ACCOUNT_NAME ,\'SPONSORED PRODUCTS\' CAMPAIGN_TYPE ,PRIMARY_KEY ,DATE ,RECORDTYPE ,b.PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,CAMPAIGNSTATUS ,KEYWORDID ,KEYWORDTEXT ,ADID ,MATCHTYPE ,TARGETID ,TARGETINGTYPE ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,ASIN ,OTHERASIN ,SKU ,IMPRESSIONS ,CLICKS ,COST ,ATTRIBUTEDSALES14D ,ATTRIBUTEDCONVERSIONS14D ,ATTRIBUTEDSALES7D ,ATTRIBUTEDCONVERSIONS7D ,ATTRIBUTEDCONVERSIONS14DSAMESKU ,ATTRIBUTEDSALES14DSAMESKU ,ATTRIBUTEDSALES14DOTHERSKU ,REGION ,STATE ,CAMPAIGNTARGETINGTYPE ,CAMPAIGN_PLACEMENT_BIDDING_PERCENTAGE ,CAMPAIGN_PLACEMENT_BIDDING ,CAMPAIGN_STRATEGY ,CAMPAIGN_BUDGET ,CAMPAIGN_BUDGET_TYPE ,FINAL_AD_CATEGORY from select_db.maplemonk.select_db_amazonads_fact_items_sp_keywords a left join ( select profileid , upper(concat(\'AMAZON_\',replace(accountinfo:name,\'\"\',\'\'))) profile_name from select_db.maplemonk.amazon_advertisement_aa_select_kyari_profiles ) b on a.PROFILEID = b.profileid union all select b.profile_name ACCOUNT_NAME ,CASE WHEN lower(CAMPAIGN_PLACEMENT_BIDDING) like \'cpc\' then \'SPONSORED DISPLAY (CPC)\' WHEN lower(CAMPAIGN_PLACEMENT_BIDDING) like \'vcpm\' then \'SPONSORED DISPLAY (vCPM)\' else \'SPONSORED DISPLAY\' end CAMPAIGN_TYPE ,PRIMARY_KEY ,DATE ,RECORDTYPE ,a.PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,CAMPAIGNSTATUS ,KEYWORDID ,KEYWORDTEXT ,ADID ,MATCHTYPE ,TARGETID ,TARGETINGTYPE ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,ASIN ,OTHERASIN ,SKU ,IMPRESSIONS ,CLICKS ,COST ,ATTRIBUTEDSALES14D ,ATTRIBUTEDCONVERSIONS14D ,ATTRIBUTEDSALES7D ,ATTRIBUTEDCONVERSIONS7D ,ATTRIBUTEDCONVERSIONS14DSAMESKU ,ATTRIBUTEDSALES14DSAMESKU ,ATTRIBUTEDSALES14DOTHERSKU ,REGION ,STATE ,CAMPAIGNTARGETINGTYPE ,CAMPAIGN_PLACEMENT_BIDDING_PERCENTAGE ,CAMPAIGN_PLACEMENT_BIDDING ,CAMPAIGN_STRATEGY ,CAMPAIGN_BUDGET ,CAMPAIGN_BUDGET_TYPE ,FINAL_AD_CATEGORY from select_db.maplemonk.select_db_amazonads_fact_items_sd_targets a left join ( select profileid , upper(concat(\'AMAZON_\',replace(accountinfo:name,\'\"\',\'\'))) profile_name from select_db.maplemonk.amazon_advertisement_aa_select_kyari_profiles ) b on a.PROFILEID = b.profileid union all select b.profile_name ACCOUNT_NAME ,\'SPONSORED BRANDS\' CAMPAIGN_TYPE ,PRIMARY_KEY ,DATE ,RECORDTYPE ,b.PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,CAMPAIGNSTATUS ,KEYWORDID ,KEYWORDTEXT ,ADID ,MATCHTYPE ,TARGETID ,TARGETINGTYPE ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,ASIN ,OTHERASIN ,SKU ,IMPRESSIONS ,CLICKS ,COST ,ATTRIBUTEDSALES14D ,ATTRIBUTEDCONVERSIONS14D ,ATTRIBUTEDSALES7D ,ATTRIBUTEDCONVERSIONS7D ,ATTRIBUTEDCONVERSIONS14DSAMESKU ,ATTRIBUTEDSALES14DSAMESKU ,ATTRIBUTEDSALES14DOTHERSKU ,REGION ,STATE ,CAMPAIGNTARGETINGTYPE ,CAMPAIGN_PLACEMENT_BIDDING_PERCENTAGE ,CAMPAIGN_PLACEMENT_BIDDING ,CAMPAIGN_STRATEGY ,CAMPAIGN_BUDGET ,CAMPAIGN_BUDGET_TYPE ,NULL as FINAL_AD_CATEGORY from SELECT_DB.MAPLEMONK.select_db_amazonads_fact_items_sb_ad a left join ( select profileid , upper(concat(\'AMAZON_\',replace(accountinfo:name,\'\"\',\'\'))) profile_name from select_db.maplemonk.amazon_advertisement_aa_select_kyari_profiles ) b on a.PROFILEID = b.profileid ;",
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
                        