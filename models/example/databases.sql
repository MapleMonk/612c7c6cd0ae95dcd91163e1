{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.CC_GA_CUSTOM_ORDER_BY_SOURCE_CONSOLIDATED AS With CustomOrder_By_SOurce as ( select * from maplemonk.cc_ga4_shy_es_order_by_custom_source union all select * from maplemonk.cc_ga4_shy_it_orderby_custom_source union all select * from maplemonk.cc_ga4_shy_uk_orderby_custom_source union all select * from maplemonk.cc_ga4_shy_dk_orderby_custom_source union all select * from maplemonk.cc_ga4_shy_de_orderby_custom_source union all select * from maplemonk.cc_ga4_shy_fr_orderby_custom_source union all select * from maplemonk.cc_ga4_shy_se_orderby_custom_source union all select * from maplemonk.cc_ga4_shy_nl_orderby_custom_source union all select * from maplemonk.cc_ga4_WW_dk_orderby_custom_source union all select * from maplemonk.cc_ga4_WW_es_orderby_custom_source union all select * from maplemonk.cc_ga4_WW_fr_orderby_custom_source union all select * from maplemonk.cc_ga4_WW_it_orderby_custom_source union all select * from maplemonk.cc_ga4_WW_se_orderby_custom_source union all select * from maplemonk.cc_ga4_WW_nl_orderby_custom_source union all select * from maplemonk.cc_ga4_WW_uk_orderby_custom_source union all select * from maplemonk.cc_ga4_SC_DE_orderby_custom_source union all select * from maplemonk.cc_ga4_animigo_de_orderby_custom_source union all select * from maplemonk.cc_ga4_animigo_uk_orderby_custom_source union all select * from maplemonk.cc_ga4_animigo_se_orderby_custom_source union all select * from maplemonk.cc_ga4_animigo_nl_orderby_custom_source union all select * from maplemonk.cc_ga4_animigo_fr_orderby_custom_source union all select * from maplemonk.cc_ga4_animigo_es_orderby_custom_source union all select * from maplemonk.cc_ga4_animigo_dk_orderby_custom_source union all select * from maplemonk.cc_ga4_animigo_it_orderby_custom_source ) select transactionId , customChannelGroup_8615248764 as customChannelGroup from CustomOrder_By_SOurce qualify row_number() over(partition by lower(transactionId) order by transactionId) = 1",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            