{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE nessa_DB.MAPLEMONK.FACEBOOK_CONSOLIDATED_nessa AS select Adset_Name,Adset_ID,FB.ad_id,FB.ad_name,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Nessa\' Facebook_Account from nessa_DB.MAPLEMONK.FBADS_NESSA_ADS_INSIGHTS FB left join ( select ad_id,ad_name,date_start ,SUM(C.value:value) Conversions from nessa_DB.MAPLEMONK.FBADS_NESSA_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,ad_name,date_start ,SUM(CV.value:value) Conversion_Value from nessa_DB.MAPLEMONK.FBADS_NESSA_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.AD_NAME, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start; create or replace TABLE nessa_db.maplemonk.FACEBOOK_CONSOLIDATED_nessa as select ADSET_NAME, ADSET_ID, AD_ID, AD_NAME, ACCOUNT_NAME, ACCOUNT_ID, CAMPAIGN_NAME, CAMPAIGN_ID, f.DATE, CLICKS, SPEND*g.close as SPEND, IMPRESSIONS, CONVERSIONS, CONVERSION_VALUE*g.close as CONVERSION_VALUE, CHANNEL, FACEBOOK_ACCOUNT from NESSA_DB.MAPLEMONK.facebook_consolidated_nessa f left join (select date::date as date, close from nessa_db.maplemonk.gsheet_exchange_rates) g on F.DATE = g.date; CREATE OR REPLACE TABLE nessa_DB.MAPLEMONK.MARKETING_CONSOLIDATED_nessa AS select ADSET_NAME, ADSET_ID,ad_id,ad_name, ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE ,NULL AD_TYPE,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE,NULL AD_URL ,NULL Day_of_Week ,YEAR(DATE) AS YEAR ,MONTH(DATE) AS MONTH ,CHANNEL ,FACEBOOK_ACCOUNT AS ACCOUNT ,SUM(CLICKS) Clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,SUM(CONVERSIONS) Conversions ,SUM(CONVERSION_VALUE) Conversion_Value from nessa_DB.MAPLEMONK.FACEBOOK_CONSOLIDATED_nessa group by ADSET_NAME, ADSET_ID,ad_id,ad_name, ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE, CHANNEL, FACEBOOK_ACCOUNT;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from NESSA_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        