{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table sleepycat_db.maplemonk.sleepycat_db_flipkart_ads_fact_items as select \'FLIPKART\' Channel ,\"Campaign ID\" as CAMPAIGN_ID ,\"Campaign Name\" as CAMPAIGN_NAME ,\"CAMPAIGN_BUDGET\" as CAMPAIGN_BUDGET ,\"CAMPAIGN_STATUS\" as CAMPAIGN_STATUS ,try_to_date(\"DATE\",\'DD-MON-YY\') as DATE ,try_cast(\"VIEWS\" as float) as Views ,try_cast(\"CLICKS\" as float) as Clicks ,try_cast(\"Ad Spend\" as float) as Spend ,try_cast(\"Direct Revenue\" as float) as Direct_Revenue ,try_cast(\"Indirect Revenue\" as float) as Indirect_Revenue ,ifnull(Direct_Revenue,0) + ifnull(Indirect_Revenue,0) Total_Revenue ,try_cast(\"Units Sold (Direct)\" as float) as Direct_Units_Sold ,try_cast(\"Units Sold (Indirect)\" as float) as Indirect_Units_Sold ,ifnull(Direct_Units_Sold,0) + ifnull(Indirect_Units_Sold,0) Total_Units_Sold from sleepycat_db.maplemonk.sleepycat_flipkart_ads_pla;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SLEEPYCAT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        