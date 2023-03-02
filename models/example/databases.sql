{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE ghc_db.MAPLEMONK.FACEBOOK_CONSOLIDATED_ghc AS select ad_name,Adset_Name,Adset_ID,Account_Name, fb.Account_ID, Campaign_Name, fb.Campaign_ID,Fb.date_start Date ,adset.daily_budget/100 budget ,SUM(inline_link_CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,sum(reach) Reach ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Mars\' Facebook_Account from ghc_db.MAPLEMONK.FB_MARS_ADS_INSIGHTS Fb left join ghc_db.maplemonk.fb_mars_ad_sets adset on adset.id = fb.adset_id left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from ghc_db.MAPLEMONK.fb_mars_ads_insights,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from ghc_db.MAPLEMONK.fb_mars_ads_insights,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by ad_name, Adset_Name, Adset_ID, Account_Name, fb.Account_ID, Campaign_Name, fb.Campaign_ID,Fb.date_start, budget union select ad_name,Adset_Name,Adset_ID,Account_Name, fb.Account_ID, Campaign_Name, fb.Campaign_ID,Fb.date_start Date ,adset.daily_budget/100 budget ,SUM(inline_link_CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,sum(reach) Reach ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Saturn\' Facebook_Account from ghc_db.MAPLEMONK.fb_saturn_ads_insights Fb left join ghc_db.maplemonk.fb_saturn_ad_sets adset on adset.id = fb.adset_id left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from ghc_db.MAPLEMONK.fb_saturn_ads_insights,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from ghc_db.MAPLEMONK.fb_saturn_ads_insights,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by ad_name,Adset_Name, Adset_ID, Account_Name, fb.Account_ID, Campaign_Name, fb.Campaign_ID,Fb.date_start, budget ; CREATE OR REPLACE TABLE ghc_db.MAPLEMONK.MARKETING_CONSOLIDATED_GHC_INTERMEDIATE AS select ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE ,NULL AD_TYPE,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE,NULL AD_URL ,NULL Day_of_Week ,YEAR(DATE) AS YEAR ,MONTH(DATE) AS MONTH ,CHANNEL ,FACEBOOK_ACCOUNT AS ACCOUNT ,SUM(CLICKS) Clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,SUM(CONVERSIONS) Conversions ,SUM(CONVERSION_VALUE) Conversion_Value ,budget from GHC_DB.MAPLEMONK.FACEBOOK_CONSOLIDATED_ghc group by ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE, CHANNEL, FACEBOOK_ACCOUNT, budget union select NULL, NULL ,\"campaign.name\", \"campaign.id\", \"segments.date\" ,NULL, NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,\'Google MARS Old\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conveersions ,sum(\"metrics.conversions_value\") conversion_value ,sum(\"campaign_budget.amount_micros\")/1000000 budget from ghc_db.maplemonk.gads_mars1_campaign_data group by \"campaign.name\", \"campaign.id\", \"segments.date\" UNION select NULL, NULL ,\"campaign.name\", \"campaign.id\", \"segments.date\" ,NULL, NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,\'Google MARS 2\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conveersions ,sum(\"metrics.conversions_value\") conversion_value ,sum(\"campaign_budget.amount_micros\")/1000000 budget from ghc_db.maplemonk.gads_mars2_campaign_data group by \"campaign.name\", \"campaign.id\", \"segments.date\" union select NULL, NULL ,\"campaign.name\", \"campaign.id\", \"segments.date\" ,NULL, NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,\'Google SATURN\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conveersions ,sum(\"metrics.conversions_value\") conversion_value ,sum(\"campaign_budget.amount_micros\")/1000000 budget from ghc_db.maplemonk.gads_saturn_campaign_data group by \"campaign.name\", \"campaign.id\", \"segments.date\" union select NULL, NULL ,\"campaign.name\", \"campaign.id\", \"segments.date\" ,NULL, NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,\'Google MARS New\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conveersions ,sum(\"metrics.conversions_value\") conversion_value ,sum(\"campaign_budget.amount_micros\")/1000000 budget from ghc_db.maplemonk.gads_7526663276_campaign_data group by \"campaign.name\", \"campaign.id\", \"segments.date\" union select NULL, NULL ,\"campaign.name\", \"campaign.id\", \"segments.date\" ,NULL, NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,\'Google Saturn 2255416600\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conveersions ,sum(\"metrics.conversions_value\") conversion_value ,sum(\"campaign_budget.amount_micros\")/1000000 budget from ghc_db.maplemonk.gads_saturn_2255416600_campaign_data group by \"campaign.name\", \"campaign.id\", \"segments.date\" ; create or replace table ghc_db.maplemonk.ga_campaign_data_saturn as select ga_date, view_id, ga_campaign, sum(ga_transactions) transactions, case when sum(ga_transactionrevenue::float) > 1000000 then 0 else sum(ga_transactionrevenue::float) end revenues from ghc_db.maplemonk.ga_saturn_orders_by_campaign_account group by 1,2,3 ; create or replace table ghc_db.maplemonk.GoogleAds_FB_GA_Saturn as select account_name, \'Saturn\' as Division, account_id, campaign_id, coalesce(c.campaign_name,d.ga_campaign) campaign_name, coalesce(c.date,d.ga_date) date, month(coalesce(c.date,d.ga_date)) month, year(coalesce(c.date,d.ga_date)) year, channel, ACCOUNT, budget, c.spend, c.impressions, c.clicks, coalesce(c.transactions, d.transactions) conversions, coalesce(c.revenues, d.revenues) conversion_value from ( select account_name, account_id, campaign_id, campaign_name, date, month, year, channel, ACCOUNT, budget, avg(clicks) clicks, avg(spend) spend, avg(impressions) impressions, sum(ifnull(b.transactions,0)) + sum(ifnull(f.transactions,0)) transactions, sum(ifnull(b.revenues,0)) + sum(ifnull(f.revenues,0)) revenues from ghc_db.MAPLEMONK.MARKETING_CONSOLIDATED_GHC_intermediate a left join (select * from ghc_db.maplemonk.ga_campaign_data_saturn where lower(left(ga_campaign,3)) not in (\'yti\',\'yt-\')) b on a.date = b.ga_Date and a.campaign_name = b.ga_campaign left join (select * from ghc_db.maplemonk.ga_campaign_data_saturn where lower(left(ga_campaign,3)) in (\'yt-\')) f on a.date = f.ga_Date and f.ga_campaign = a.campaign_name where account in (\'Google SATURN\', \'Google Saturn 2255416600\') group by 1,2,3,4,5,6,7,8,9,10 order by campaign_name ) c full outer join ( select ga_date, ga_campaign, split_part(ga_campaign,\'-\',2) influencer, split_part(ga_campaign,\'-\',3) category, split_part(ga_campaign,\'-\',4) product, transactions, revenues from ghc_db.maplemonk.ga_campaign_data_saturn where transactions <> 0 and lower(left(ga_campaign,3)) = \'yti\' )d on c.date = d.ga_date and replace(lower(c.campaign_name),\'-\',\'\') like \'%\'||lower(d.influencer)||\'%\' and replace(lower(c.campaign_name),\'-\',\'\') like \'%\'||lower(d.category)||\'%\' and replace(lower(c.campaign_name),\'-\',\'\') like \'%\'||lower(d.product)||\'%\' union all select account_name, \'Saturn\' as Division, account_id, campaign_id, ad_name campaign_name, Date, month(date) month, year(date) year, \'Facebook\' as Channel, \'Facebook Saturn\' as Account, budget, SUM(SPEND) Spend, SUM(IMPRESSIONS) Impressions, SUM(CLICKS) Clicks, sum(conversions) Conversions, sum(conversion_value) conversion_value from ghc_db.MAPLEMONK.facebook_consolidated_ghc a left join (select * from ghc_db.maplemonk.ga_campaign_data_saturn where lower(left(ga_campaign,3)) <> \'yti\') b on a.date = b.ga_Date and a.ad_name = b.ga_campaign where spend <> 0 and a.facebook_account = \'Facebook Saturn\' group by 1,2,3,4,5,6,7,8,9,10 ,11 ; create or replace table ghc_db.maplemonk.ga_campaign_data_mars as select ga_date, view_id, ga_campaign, sum(ga_transactions) transactions, case when sum(ga_transactionrevenue::float) > 1000000 then 0 else sum(ga_transactionrevenue::float) end revenues from ghc_db.maplemonk.ga_mars_orders_by_campaign_account group by 1,2,3 ; create or replace table ghc_db.maplemonk.GoogleAds_FB_GA_Mars as select account_name, \'Mars\' as Division, account_id, campaign_id, coalesce(c.campaign_name, d.ga_campaign) campaign_name, coalesce(c.date,d.ga_date) date, month(coalesce(c.date,d.ga_date)) month, year(coalesce(c.date,d.ga_date)) year, channel, ACCOUNT, budget, c.spend, c.impressions, c.clicks, coalesce(c.transactions, d.transactions) conversions, coalesce(c.revenues, d.revenues) conversion_value from ( select account_name, account_id, campaign_id, campaign_name, date, month, year, channel, ACCOUNT, budget, avg(clicks) clicks, avg(spend) spend, avg(impressions) impressions, sum(ifnull(b.transactions,0)) + sum(ifnull(f.transactions,0)) transactions, sum(ifnull(b.revenues,0)) + sum(ifnull(f.revenues,0)) revenues from ghc_db.MAPLEMONK.MARKETING_CONSOLIDATED_GHC_intermediate a left join (select * from ghc_db.maplemonk.ga_campaign_data_mars where lower(left(ga_campaign,3)) not in (\'yti\',\'yt-\') ) b on a.date = b.ga_Date and a.campaign_name = b.ga_campaign left join (select * from ghc_db.maplemonk.ga_campaign_data_mars where lower(left(ga_campaign,3)) in (\'yt-\') ) f on a.date = f.ga_Date and f.ga_campaign = a.campaign_name where account in (\'Google MARS Old\',\'Google MARS New\',\'Google MARS 2\') group by 1,2,3,4,5,6,7,8,9,10 order by campaign_name ) c full outer join ( select ga_date, ga_campaign, split_part(ga_campaign,\'-\',2) influencer, split_part(ga_campaign,\'-\',3) category, split_part(ga_campaign,\'-\',4) product, transactions, revenues from ghc_db.maplemonk.ga_campaign_data_mars where transactions <> 0 and lower(left(ga_campaign,3)) = \'yti\' )d on c.date = d.ga_date and replace(lower(c.campaign_name),\'-\',\'\') like \'%\'||lower(d.influencer)||\'%\' and replace(lower(c.campaign_name),\'-\',\'\') like \'%\'||lower(d.category)||\'%\' and replace(lower(c.campaign_name),\'-\',\'\') like \'%\'||lower(d.product)||\'%\' union all select account_name, \'Mars\' as division, account_id, campaign_id, ad_name campaign_name, date Date, month(date) month, year(date) year, \'Facebook\' as Channel, \'Facebook Mars\' as Account, budget, SUM(SPEND) Spend, SUM(IMPRESSIONS) Impressions, SUM(CLICKS) Clicks, sum(transactions) Conversions, sum(revenues) conversion_value from ghc_db.MAPLEMONK.facebook_consolidated_ghc a left join (select * from ghc_db.maplemonk.ga_campaign_data_mars where lower(left(ga_campaign,3)) <> \'yti\') b on a.date = b.ga_Date and a.ad_name = b.ga_campaign where spend <> 0 and a.facebook_account = \'Facebook Mars\' group by 1,2,3,4,5,6,7,8,9,10,11 ; create or replace table ghc_db.MAPLEMONK.MARKETING_CONSOLIDATED_GHC_1 as select a.*, case when lower(account) like \'%mars%\' and lower(campaign_name) like \'%beard%\' then \'Beard\' when lower(account) like \'%mars%\' and lower(campaign_name) like \'%hair%\' then \'Hair\' when lower(account) like \'%mars%\' and lower(campaign_name) like \'%wellness%\' then \'Wellness\' when lower(account) like \'%mars%\' and lower(campaign_name) like \'%performance%\' then \'Performance\' when lower(account) like \'%mars%\' and lower(campaign_name) like \'%skin%\' then \'Skin\' when lower(account) is null and lower(campaign_name) like \'%beard%\' then \'Beard\' when lower(account) is null and lower(campaign_name) like \'%hair%\' then \'Hair\' when lower(account) is null and lower(campaign_name) like \'%wellness%\' then \'Wellness\' when lower(account) is null and lower(campaign_name) like \'%performance%\' then \'Performance\' when lower(account) is null and lower(campaign_name) like \'%skin%\' then \'Skin\' when lower(account) like \'%saturn%\' and lower(campaign_name) like \'%skin%\' then \'Skin\' when lower(account) like \'%saturn%\' and lower(campaign_name) like \'%hair%\' then \'Hair\' when lower(account) like \'%saturn%\' and lower(campaign_name) like \'%wellness%\' then \'Wellness\' end as Category, ifnull(b.shopify_orders,0) + ifnull(c.shopify_orders,0) shopify_orders, ifnull(b.shopify_revenue,0) + ifnull(c.shopify_revenue,0) shopify_revenue from ( select * from ghc_db.maplemonk.GoogleAds_FB_GA_Saturn union all select null as account_name, \'Saturn\' as Division, null as account_id, null as campaign_id, ga_campaign campaign_name, ga_date date, month(ga_date) month, year(ga_date) year, null as channel, null as account, null as budget, null as spend, null as impressions, null as clicks, transactions conversions, revenues conversion_value from ghc_db.maplemonk.ga_campaign_data_saturn where transactions <> 0 and concat(ga_date,ga_campaign) not in (select distinct concat(date,campaign_name) from ghc_db.maplemonk.GoogleAds_FB_GA_Saturn ) Union all select * from ghc_db.maplemonk.GoogleAds_FB_GA_Mars union all select null as account_name, \'Mars\' as Division, null as account_id, null as campaign_id, ga_campaign campaign_name, ga_date date, month(ga_date) month, year(ga_date) year, null as channel, null as account, null as budget, null as spend, null as impressions, null as clicks, transactions conversions, revenues conversion_value from ghc_db.maplemonk.ga_campaign_data_mars where transactions <> 0 and concat(ga_date,ga_campaign) not in (select distinct concat(date,campaign_name) from ghc_db.maplemonk.GoogleAds_FB_GA_Mars ) ) a left join (select order_timestamp::date date,landing_utm_campaign campaign, count(distinct order_id) shopify_orders, sum(gross_sales_after_tax) shopify_revenue from ghc_db.maplemonk.fact_items_shopify_ghc group by 1,2) b on a.date = b.date and a.campaign_name = b.campaign left join (select order_timestamp::date date,referring_utm_campaign campaign, count(distinct order_id) shopify_orders, sum(gross_sales_after_tax) shopify_revenue from ghc_db.maplemonk.fact_items_shopify_ghc group by 1,2) c on a.date = c.date and a.campaign_name = c.campaign ; create or replace table ghc_db.MAPLEMONK.MARKETING_CONSOLIDATED_GHC as select division, campaign_name, date, month, year, channel, account, budget, category, sum(ifnull(spend,0)) spend, sum(ifnull(impressions,0)) impressions, sum(ifnull(clicks,0)) clicks, sum(ifnull(conversions,0)) conversions, sum(ifnull(conversion_value,0)) conversion_value, sum(ifnull(shopify_orders,0)) shopify_orders, sum(ifnull(shopify_revenue,0)) shopify_revenue from( select coalesce(c.account_name,d.account_name) account_name, coalesce(c.division,d.division) division, c.account_id, c.campaign_id, coalesce(c.campaign_name,d.campaign_name) campaign_name, date, month, year, coalesce(c.channel,d.channel) channel, coalesce(c.account, d.account) account, coalesce(c.budget, d.budget) budget, coalesce(c.category, d.category) category, spend, impressions, clicks, conversions, conversion_value, shopify_orders, shopify_revenue from ( select a.account_name, a.division, a.account_id, a.campaign_id, coalesce(b.campaign,a.campaign_name) campaign_name, a.date, a.month, a.year, a.channel, a.account, a.budget, a.spend, a.impressions, a.clicks, a.conversions, a.conversion_value, a.category, a.shopify_orders, a.shopify_revenue from ghc_db.maplemonk.marketing_consolidated_ghc_1 a left join ghc_db.maplemonk.campaign_with_utm b on a.campaign_name = b.\"YTI-Adset\" ) c left join ( select distinct account_name,division, campaign_name, channel, account, category, budget from ghc_db.maplemonk.marketing_consolidated_ghc_1 where lower(left(campaign_name,3)) <> \'yti\' and channel is not null ) d on c.campaign_name = d.campaign_name and c.division = d.division ) group by 1,2,3,4,5,6,7,8,9 ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from GHC_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        