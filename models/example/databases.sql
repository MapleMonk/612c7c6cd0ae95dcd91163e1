{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table datalake_db.kaya.dwh_missing_utm_mappings as With Id_With_Sales as ( select id, name, created_at::date date, sum(ifnull(TOTAL_PRICE, 0)) TOTAL_PRICE from datalake_db.kaya.trn_Shopify_kaya_ORDERS group by 1, 2, 3 ), Sales as ( select date, upper(GOKWIK_UTM_SOURCE) as GOKWIK_UTM_SOURCE, upper(GOKWIK_UTM_MEDIUM) as GOKWIK_UTM_MEDIUM, upper(gokwik_utm_campaign) as gokwik_utm_campaign, sum(ifnull(TOTAL_PRICE, 0)) as Sales from datalake_db.kaya.dwh_GOKWIK_SOURCE GS left join Id_With_Sales iw on gs.id = iw.id group by 1, 2, 3, 4 ), UTM_Mapping as ( select distinct a.utm_source, a.utm_medium, a.utm_campaign from ( select distinct id::string, GOKWIK_UTM_SOURCE utm_source, GOKWIK_UTM_MEDIUM utm_medium, gokwik_utm_campaign utm_campaign from datalake_db.kaya.dwh_GOKWIK_SOURCE union select distinct name, ShopifyQL_Unmapped_Last_Source, Last_Moment_UTM_Medium, lastvisit_utm_campaign from datalake_db.kaya.dwh_Shopify_kaya_UTM_Parameters ) a left join ( select * from ( select *, row_number() over ( partition by lower(utm_source), lower(utm_medium), lower(utm_campaign) order by 1 ) rw from datalake_db.kaya.kaya_db_orders_utm_mapping ) where rw = 1 and utm_source is not null ) UTM_MAPPING on lower(ifnull(a.utm_source, \'\')) = lower(ifnull(utm_mapping.utm_source, \'\')) and lower(ifnull(a.utm_medium, \'\')) = lower(ifnull(utm_mapping.utm_medium, \'\')) and lower(ifnull(a.utm_campaign, \'\')) = lower(ifnull(utm_mapping.utm_campaign, \'\')) where utm_mapping.channel is null ) select utm.*, s.sales, date from UTM_Mapping utm left join Sales S on lower(ifnull(utm.utm_source, \'\')) = lower(ifnull(s.GOKWIK_UTM_SOURCE, \'\')) and lower(ifnull(utm.utm_medium, \'\')) = lower(ifnull(s.GOKWIK_UTM_MEDIUM, \'\')) and lower(ifnull(utm.utm_campaign, \'\')) = lower(ifnull(s.GOKWIK_UTM_campaign, \'\'));",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from DATALAKE_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            