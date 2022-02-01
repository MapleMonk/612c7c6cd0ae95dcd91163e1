{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.AMAZONADS_NA_MARKETING AS SELECT 'Sponsored Products' CAMPAIGN_TYPE, sp.PROFILEID ,CAST((sp.REPORTDATE - 1) AS INT) as DATE ,sp.metric:campaignId::int as CampaignID ,sp.metric:campaignName::varchar as CampaignName ,sp.metric:adGroupId::int as AdGroupId ,sp.metric:adGroupName::varchar as AdGroupName ,sp.metric:keywordText::varchar as Keywordtext ,sp.metric:campaignStatus::varchar as CampaignStatus ,map.ASIN as Asin ,map."product name" as ProductName ,map."Category 1" as ProductCategory ,sp.metric:adId::varchar as AdId ,sp.metric:targetingExpression::varchar as TargetingExpression ,sp.metric:targetingText::varchar as TargetingText ,sp.metric:currency::varchar as Currency ,LEFT(sp.REPORTDATE, 4) as Year ,SUBSTR(sp.REPORTDATE, 5, 2) AS Month ,SUBSTR(sp.REPORTDATE, 7, 2) AS Day ,sum(sp.metric:impressions::int) as Impressions ,sum(sp.metric:clicks::int) as Clicks ,sum(sp.metric:cost::int) as Spend ,sum(sp.metric:attributedSales14d::int) as Sales ,sum(sp.metric:attributedConversions14d::int) as Conversions FROM VAHDAM_DB.MAPLEMONK.AMAZONADS_NA_SPONSORED_PRODUCTS_REPORT_STREAM sp LEFT JOIN (SELECT DISTINCT "(Child) ASIN" as ASIN, b.* FROM VAHDAM_DB.MAPLEMONK.AMAZON_USA_MANUAL_SKU_MAPPING a LEFT JOIN VAHDAM_DB.MAPLEMONK.SHOPIFY_SKU_MAPPING b ON a."Common SKU Id's"= b."common sku") map ON sp.metric:asin::varchar = map.ASIN WHERE sp.RECORDTYPE = 'productAds' and sp.PROFILEID = 4287412371422290 GROUP BY sp.REPORTDATE, sp.PROFILEID, sp.metric:campaignId::int, sp.metric:campaignName::varchar ,sp.metric:adGroupId::int, sp.metric:adGroupName::varchar, sp.metric:keywordText::varchar ,sp.metric:campaignStatus::varchar, map.ASIN, map."product name", map."Category 1" ,sp.metric:adId::varchar, sp.metric:targetingExpression::varchar, sp.metric:targetingText::varchar ,sp.metric:targetingType::varchar, sp.metric:currency::varchar UNION SELECT 'Sponsored Display' CAMPAIGN_TYPE, sd.PROFILEID ,CAST((sd.REPORTDATE - 1) AS INT) as DATE ,sd.metric:campaignId::int as CampaignID ,sd.metric:campaignName::varchar as CampaignName ,sd.metric:adGroupId::int as AdGroupId ,sd.metric:adGroupName::varchar as AdGroupName ,sd.metric:keywordText::varchar as Keywordtext ,sd.metric:campaignStatus::varchar as CampaignStatus ,map.ASIN as Asin ,map."product name" as ProductName ,map."Category 1" as ProductCategory ,sd.metric:adId::varchar as AdId ,sd.metric:targetingExpression::varchar as TargetingExpression ,sd.metric:targetingText::varchar as TargetingText ,sd.metric:currency::varchar as Currency ,LEFT(sd.REPORTDATE, 4) as Year ,SUBSTR(sd.REPORTDATE, 5, 2) AS Month ,SUBSTR(sd.REPORTDATE, 7, 2) AS Day ,sum(sd.metric:impressions::int) as Impressions ,sum(sd.metric:clicks::int) as Clicks ,sum(sd.metric:cost::int) as Spend ,sum(sd.metric:attributedSales14d::int) as Sales ,sum(sd.metric:attributedConversions14d::int) as Conversions FROM VAHDAM_DB.MAPLEMONK.AMAZONADS_NA_SPONSORED_DISPLAY_REPORT_STREAM sd LEFT JOIN (SELECT DISTINCT "(Child) ASIN" as ASIN, b.* FROM VAHDAM_DB.MAPLEMONK.AMAZON_USA_MANUAL_SKU_MAPPING a LEFT JOIN VAHDAM_DB.MAPLEMONK.SHOPIFY_SKU_MAPPING b ON a."Common SKU Id's"= b."common sku") map ON sd.metric:asin::varchar = map.ASIN WHERE sd.RECORDTYPE = 'productAds' and sd.PROFILEID = 4287412371422290 GROUP BY sd.REPORTDATE, sd.PROFILEID, sd.metric:campaignId::int, sd.metric:campaignName::varchar ,sd.metric:adGroupId::int, sd.metric:adGroupName::varchar, sd.metric:keywordText::varchar ,sd.metric:campaignStatus::varchar, map.ASIN, map."product name", map."Category 1" ,sd.metric:adId::varchar, sd.metric:targetingExpression::varchar, sd.metric:targetingText::varchar ,sd.metric:targetingType::varchar, sd.metric:currency::varchar UNION SELECT 'Sponsored Brands' CAMPAIGN_TYPE, sb.PROFILEID ,CAST((sb.REPORTDATE - 1) AS INT) as DATE ,sb.metric:campaignId::int as CampaignID ,sb.metric:campaignName::varchar as CampaignName ,sb.metric:adGroupId::int as AdGroupId ,sb.metric:adGroupName::varchar as AdGroupName ,sb.metric:keywordText::varchar as Keywordtext ,sb.metric:campaignStatus::varchar as CampaignStatus ,map.ASIN as Asin ,map."product name" as ProductName ,map."Category 1" as ProductCategory ,sb.metric:adId::varchar as AdId ,sb.metric:targetingExpression::varchar as TargetingExpression ,sb.metric:targetingText::varchar as TargetingText ,sb.metric:currency::varchar as Currency ,LEFT(sb.REPORTDATE, 4) as Year ,SUBSTR(sb.REPORTDATE, 5, 2) AS Month ,SUBSTR(sb.REPORTDATE, 7, 2) AS Day ,sum(sb.metric:impressions::int) as Impressions ,sum(sb.metric:clicks::int) as Clicks ,sum(sb.metric:cost::int) as Spend ,sum(sb.metric:attributedSales14d::int) as Sales ,sum(sb.metric:attributedConversions14d::int) as Conversions FROM VAHDAM_DB.MAPLEMONK.AMAZONADS_NA_SPONSORED_BRANDS_REPORT_STREAM sb LEFT JOIN (SELECT DISTINCT "(Child) ASIN" as ASIN, b.* FROM VAHDAM_DB.MAPLEMONK.AMAZON_USA_MANUAL_SKU_MAPPING a LEFT JOIN VAHDAM_DB.MAPLEMONK.SHOPIFY_SKU_MAPPING b ON a."Common SKU Id's"= b."common sku") map ON sb.metric:asin::varchar = map.ASIN WHERE sb.RECORDTYPE = 'keywords' and sb.PROFILEID = 4287412371422290 GROUP BY sb.REPORTDATE, sb.PROFILEID, sb.metric:campaignId::int, sb.metric:campaignName::varchar ,sb.metric:adGroupId::int, sb.metric:adGroupName::varchar, sb.metric:keywordText::varchar ,sb.metric:campaignStatus::varchar, map.ASIN, map."product name", map."Category 1" ,sb.metric:adId::varchar, sb.metric:targetingExpression::varchar, sb.metric:targetingText::varchar ,sb.metric:targetingType::varchar, sb.metric:currency::varchar UNION SELECT 'Sponsored Brands Video' CAMPAIGN_TYPE, sbv.PROFILEID ,CAST((sbv.REPORTDATE - 1) AS INT) as DATE ,sbv.metric:campaignId::int as CampaignID ,sbv.metric:campaignName::varchar as CampaignName ,sbv.metric:adGroupId::int as AdGroupId ,sbv.metric:adGroupName::varchar as AdGroupName ,sbv.metric:keywordText::varchar as Keywordtext ,sbv.metric:campaignStatus::varchar as CampaignStatus ,map.ASIN as Asin ,map."product name" as ProductName ,map."Category 1" as ProductCategory ,sbv.metric:adId::varchar as AdId ,sbv.metric:targetingExpression::varchar as TargetingExpression ,sbv.metric:targetingText::varchar as TargetingText ,sbv.metric:currency::varchar as Currency ,LEFT(sbv.REPORTDATE, 4) as Year ,SUBSTR(sbv.REPORTDATE, 5, 2) AS Month ,SUBSTR(sbv.REPORTDATE, 7, 2) AS Day ,sum(sbv.metric:impressions::int) as Impressions ,sum(sbv.metric:clicks::int) as Clicks ,sum(sbv.metric:cost::int) as Spend ,sum(sbv.metric:attributedSales14d::int) as Sales ,sum(sbv.metric:attributedConversions14d::int) as Conversions FROM VAHDAM_DB.MAPLEMONK.AMAZONADS_NA_SPONSORED_BRANDS_VIDEO_REPORT_STREAM sbv LEFT JOIN (SELECT DISTINCT "(Child) ASIN" as ASIN, b.* FROM VAHDAM_DB.MAPLEMONK.AMAZON_USA_MANUAL_SKU_MAPPING a LEFT JOIN VAHDAM_DB.MAPLEMONK.SHOPIFY_SKU_MAPPING b ON a."Common SKU Id's"= b."common sku") map ON sbv.metric:asin::varchar = map.ASIN WHERE sbv.RECORDTYPE = 'keywords' and sbv.PROFILEID = 4287412371422290 GROUP BY sbv.REPORTDATE, sbv.PROFILEID, sbv.metric:campaignId::int, sbv.metric:campaignName::varchar ,sbv.metric:adGroupId::int, sbv.metric:adGroupName::varchar, sbv.metric:keywordText::varchar ,sbv.metric:campaignStatus::varchar, map.ASIN, map."product name", map."Category 1" ,sbv.metric:adId::varchar, sbv.metric:targetingExpression::varchar, sbv.metric:targetingText::varchar ,sbv.metric:targetingType::varchar, sbv.metric:currency::varchar",
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
            