{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table prd_db.justherbs.dwh_missing_utm_mappings as select distinct a.utm_source, a.utm_medium from (select distinct GOKWIK_UTM_SOURCE utm_source, GOKWIK_UTM_MEDIUM utm_medium from prd_db.justherbs.dwh_GOKWIK_SOURCE union select distinct ShopifyQL_Unmapped_Last_Source, Last_Moment_UTM_Medium from prd_db.justherbs.dwh_Shopify_jh_UTM_Parameters ) a left join (select * from (select * , row_number() over (partition by lower(utm_source), lower(utm_medium) order by 1) rw from datalake_db.justherbs.mst_utm_mapping) where rw=1 and utm_source is not null ) UTM_MAPPING on lower(ifnull(a.utm_source,\'\')) = lower(ifnull(utm_mapping.utm_source,\'\')) and lower(ifnull(a.utm_medium,\'\')) = lower(ifnull(utm_mapping.utm_medium,\'\')) where utm_mapping.channel is null ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from PRD_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            