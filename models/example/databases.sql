{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table prd_db.justherbs.dwh_missing_utm_mappings as With Id_With_Sales as ( select id,name,sum(ifnull(TOTAL_PRICE,0)) TOTAL_PRICE from datalake_db.justherbs.trn_Shopify_jh_ORDERS group by 1,2 ), Sales as ( select upper(GOKWIK_UTM_SOURCE)as GOKWIK_UTM_SOURCE , upper(GOKWIK_UTM_MEDIUM) as GOKWIK_UTM_MEDIUM, sum(ifnull(TOTAL_PRICE,0)) as Sales from prd_db.justherbs.dwh_GOKWIK_SOURCE GS left join Id_With_Sales iw on gs.id = iw.id group by 1,2 ) , UTM_Mapping as ( select distinct a.utm_source, a.utm_medium from (select distinct id, GOKWIK_UTM_SOURCE utm_source, GOKWIK_UTM_MEDIUM utm_medium from (select * from prd_db.justherbs.dwh_GOKWIK_SOURCE where id not in (select distinct id from datalake_db.justherbs.trn_Shopify_jh_ORDERS where name in (select distinct txn_id from datalake_db.justherbs.trn_trackier_data) ) ) union select distinct name,ShopifyQL_Unmapped_Last_Source, Last_Moment_UTM_Medium from (select * from prd_db.justherbs.dwh_Shopify_jh_UTM_Parameters where name not in (select distinct txn_id from datalake_db.justherbs.trn_trackier_data) ) ) a left join (select * from (select * , row_number() over (partition by lower(utm_source), lower(utm_medium) order by 1) rw from datalake_db.justherbs.mst_utm_mapping) where rw=1 and utm_source is not null ) UTM_MAPPING on lower(ifnull(a.utm_source,\'\')) = lower(ifnull(utm_mapping.utm_source,\'\')) and lower(ifnull(a.utm_medium,\'\')) = lower(ifnull(utm_mapping.utm_medium,\'\')) where utm_mapping.channel is null ) select utm.*,s.sales from UTM_Mapping utm left join Sales S on lower(ifnull(utm.utm_source,\'\')) = lower(ifnull(s.GOKWIK_UTM_SOURCE,\'\')) and lower(ifnull(utm.utm_medium,\'\')) = lower(ifnull(s.GOKWIK_UTM_MEDIUM,\'\')) ;",
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
            