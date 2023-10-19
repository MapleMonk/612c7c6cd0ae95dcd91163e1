{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table Perfora_DB.MAPLEMONK.PERFORA_DB_AMAZONADS_MARKETING As select DATE ,\'AMAZON_IN_PERFORA\' MARKETPLACE ,ASIN ,CAMPAIGN_TYPE ,PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,KEYWORDTEXT ,CAMPAIGNSTATUS ,ADID ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,NEWTOBRANDORDERS ,NEWTOBRANDSALES ,NEWTOBRANDUNITS ,IMPRESSIONS ,CLICKS ,SPEND ,ADSALES ,CONVERSIONS ,CONVERSIONSSAMESKU ,SALESSAMESKU ,OTHERSKUSALES ,CONVERSIONSOTHERSKU from Perfora_DB.MAPLEMONK.PERFORA_DB_AMAZONADS_MARKETING_PV2 where DATE < \'2023-10-01\' union all select DATE ,\'AMAZON_IN_PERFORA\' MARKETPLACE ,ASIN ,CAMPAIGN_TYPE ,PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,KEYWORDTEXT ,CAMPAIGNSTATUS ,ADID ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,NEWTOBRANDORDERS ,NEWTOBRANDSALES ,NEWTOBRANDUNITS ,IMPRESSIONS ,CLICKS ,SPEND ,ADSALES ,CONVERSIONS ,CONVERSIONSSAMESKU ,SALESSAMESKU ,OTHERSKUSALES ,CONVERSIONSOTHERSKU from Perfora_DB.MAPLEMONK.PERFORA_DB_AMAZONADS_MARKETING_PV3 Where DATE >= \'2023-10-01\' union all select DATE ,\'AMAZON_US_PERFORA\' MARKETPLACE ,ASIN ,CAMPAIGN_TYPE ,PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,KEYWORDTEXT ,CAMPAIGNSTATUS ,ADID ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,NEWTOBRANDORDERS ,NEWTOBRANDSALES ,NEWTOBRANDUNITS ,IMPRESSIONS ,CLICKS ,SPEND ,ADSALES ,CONVERSIONS ,CONVERSIONSSAMESKU ,SALESSAMESKU ,OTHERSKUSALES ,CONVERSIONSOTHERSKU from PERFORA_DB.MAPLEMONK.PERFORA_DB_US_AMAZONADS_MARKETING;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from Perfora_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        