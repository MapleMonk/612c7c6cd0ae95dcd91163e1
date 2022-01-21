{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.AMAZONADS_NA_MARKETING as SELECT 'Sponsored Brands Video' CAMPAIGN_TYPE, REPORTDATE ,metric:campaignId::int as CampaignID ,metric:campaignName::varchar as CampaignName ,metric:adGroupId::int as AdGroupId ,metric:adGroupName::varchar as AdGroupName ,metric:keywordText::varchar as Keywordtext ,metric:campaignStatus::varchar as CampaignStatus ,metric:adId::varchar as AdId ,metric:targetingExpression::varchar as TargetingExpression ,metric:targetingText::varchar as TargetingText ,metric:currency::varchar as Currency ,sum(metric:impressions::int) as Impressions ,sum(metric:clicks::int) as Clicks ,sum(metric:cost::int) as Spend ,sum(metric:attributedSales14d::int) as Sales ,sum(metric:attributedConversions14d::int) as Conversions FROM VAHDAM_DB.MAPLEMONK.AMAZONADS_NA_SPONSORED_BRANDS_VIDEO_REPORT_STREAM WHERE RECORDTYPE = 'keywords' GROUP BY REPORTDATE, metric:campaignId::int, metric:campaignName::varchar, metric:adGroupId::int, metric:adGroupName::varchar ,metric:keywordText::varchar, metric:campaignStatus::varchar, metric:adId::varchar, metric:targetingExpression::varchar ,metric:targetingText::varchar, metric:targetingType::varchar, metric:currency::varchar UNION SELECT 'Sponsored Brands' CAMPAIGN_TYPE, REPORTDATE ,metric:campaignId::int as CampaignID ,metric:campaignName::varchar as CampaignName ,metric:adGroupId::int as AdGroupId ,metric:adGroupName::varchar as AdGroupName ,metric:keywordText::varchar as Keywordtext ,metric:campaignStatus::varchar as CampaignStatus ,metric:adId::varchar as AdId ,metric:targetingExpression::varchar as TargetingExpression ,metric:targetingText::varchar as TargetingText ,metric:currency::varchar as Currency ,sum(metric:impressions::int) as Impressions ,sum(metric:clicks::int) as Clicks ,sum(metric:cost::int) as Spend ,sum(metric:attributedSales14d::int) as Sales ,sum(metric:attributedConversions14d::int) as Conversions FROM VAHDAM_DB.MAPLEMONK.AMAZONADS_NA_SPONSORED_BRANDS_REPORT_STREAM WHERE RECORDTYPE = 'keywords' GROUP BY REPORTDATE, metric:campaignId::int, metric:campaignName::varchar, metric:adGroupId::int, metric:adGroupName::varchar ,metric:keywordText::varchar, metric:campaignStatus::varchar, metric:adId::varchar, metric:targetingExpression::varchar ,metric:targetingText::varchar, metric:targetingType::varchar, metric:currency::varchar UNION SELECT 'Sponsored Display' CAMPAIGN_TYPE, REPORTDATE ,metric:campaignId::int as CampaignID ,metric:campaignName::varchar as CampaignName ,metric:adGroupId::int as AdGroupId ,metric:adGroupName::varchar as AdGroupName ,metric:keywordText::varchar as Keywordtext ,metric:campaignStatus::varchar as CampaignStatus ,metric:adId::varchar as AdId ,metric:targetingExpression::varchar as TargetingExpression ,metric:targetingText::varchar as TargetingText ,metric:currency::varchar as Currency ,sum(metric:impressions::int) as Impressions ,sum(metric:clicks::int) as Clicks ,sum(metric:cost::int) as Spend ,sum(metric:attributedSales14d::int) as Sales ,sum(metric:attributedConversions14d::int) as Conversions FROM VAHDAM_DB.MAPLEMONK.AMAZONADS_NA_SPONSORED_DISPLAY_REPORT_STREAM WHERE RECORDTYPE = 'targets' GROUP BY REPORTDATE, metric:campaignId::int, metric:campaignName::varchar, metric:adGroupId::int, metric:adGroupName::varchar ,metric:keywordText::varchar, metric:campaignStatus::varchar, metric:adId::varchar, metric:targetingExpression::varchar ,metric:targetingText::varchar, metric:targetingType::varchar, metric:currency::varchar",
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
            