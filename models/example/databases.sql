{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE BSC_DB.MAPLEMONK.Bombae_GOOGLEADS_CONSOLIDATED AS select NULL ADSET_NAME ,NULL ADSET_ID ,NULL AD_ID ,NULL AD_NAME ,\'Google_Bombae_GADS\' as ACCOUNT_NAME ,NULL ACCOUNT_ID ,\"campaign.name\" Campaign_name ,\"campaign.id\" Campaign_id ,\"segments.date\" date ,NULL AD_TYPE ,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE ,NULL AD_FINAL_URL ,NULL DAY_OF_WEEK ,YEAR(\"segments.date\"::date) YEAR1 ,MONTH(\"segments.date\"::date) MONTH1 ,\'PAID GOOGLE\' Channel ,\'Google_Bombae_GADS\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conversions ,sum(\"metrics.conversions_value\") conversion_value from BSC_DB.MAPLEMONK.Bombae_GADS_campaign_data group by \"campaign.name\", \"campaign.id\", \"segments.date\" ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from BSC_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        