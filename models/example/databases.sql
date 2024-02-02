{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE BSC_DB.MAPLEMONK.BSC_DB_GOOGLEADS_CONSOLIDATED AS select NULL ,NULL ,NULL ,NULL ,\'Google_BSC_GADS\' as ACCOUNT_NAME ,NULL ,\"campaign.name\" ,\"campaign.id\" ,\"segments.date\" ,NULL ,NULL ,NULL ,NULL ,NULL ,YEAR(\"segments.date\"::date) YEAR1 ,MONTH(\"segments.date\"::date) MONTH1 ,\'PAID GOOGLE\' Channel ,\'Google_BSC_GADS\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conversions ,sum(\"metrics.conversions_value\") conversions_value from BSC_DB.MAPLEMONK.BSC_GADS_campaign_data group by \"campaign.name\", \"campaign.id\", \"segments.date\";",
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
                        