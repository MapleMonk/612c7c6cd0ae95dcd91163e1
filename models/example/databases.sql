{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table prd_db.justherbs.dwh_missing_utm_mappings as With Id_With_Sales as ( select id,name,created_at::date date, sum(ifnull(TOTAL_PRICE,0)) TOTAL_PRICE from datalake_db.justherbs.trn_Shopify_jh_ORDERS group by 1,2,3 ), Sales as ( select date, upper(GOKWIK_UTM_SOURCE)as GOKWIK_UTM_SOURCE , upper(GOKWIK_UTM_MEDIUM) as GOKWIK_UTM_MEDIUM, upper(gokwik_utm_campaign) as gokwik_utm_campaign, sum(ifnull(TOTAL_PRICE,0)) as Sales from prd_db.justherbs.dwh_GOKWIK_SOURCE GS left join Id_With_Sales iw on gs.id = iw.id group by 1,2,3,4 ) , UTM_Mapping as ( select distinct a.utm_source, a.utm_medium, a.utm_campaign from (select distinct id::string, GOKWIK_UTM_SOURCE utm_source, GOKWIK_UTM_MEDIUM utm_medium, gokwik_utm_campaign utm_campaign from (select * from prd_db.justherbs.dwh_GOKWIK_SOURCE where id not in (select distinct id from datalake_db.justherbs.trn_Shopify_jh_ORDERS where name in (select distinct txn_id from datalake_db.justherbs.trn_s3_trackier_data) ) ) union select distinct name,ShopifyQL_Unmapped_Last_Source, Last_Moment_UTM_Medium, lastvisit_utm_campaign from (select * from prd_db.justherbs.dwh_Shopify_jh_UTM_Parameters where name not in (select distinct txn_id from datalake_db.justherbs.trn_s3_trackier_data) ) ) a left join (select * from (select * , row_number() over (partition by lower(utm_source), lower(utm_medium) order by 1) rw from datalake_db.justherbs.mst_utm_mapping) where rw=1 and utm_source is not null ) UTM_MAPPING on lower(ifnull(a.utm_source,\'\')) = lower(ifnull(utm_mapping.utm_source,\'\')) and lower(ifnull(a.utm_medium,\'\')) = lower(ifnull(utm_mapping.utm_medium,\'\')) where utm_mapping.channel is null ) select utm.*,s.sales,date from UTM_Mapping utm left join Sales S on lower(ifnull(utm.utm_source,\'\')) = lower(ifnull(s.GOKWIK_UTM_SOURCE,\'\')) and lower(ifnull(utm.utm_medium,\'\')) = lower(ifnull(s.GOKWIK_UTM_MEDIUM,\'\')) and lower(ifnull(utm.utm_campaign,\'\')) = lower(ifnull(s.GOKWIK_UTM_campaign,\'\')) ; create or replace table prd_db.justherbs.dwh_utm_mappings_others as select order_timestamp::Date ordeR_Date, coalesce(GOKWIK_UTM_SOURCE,SHOPIFYQL_LAST_VISIT_NON_UTM_SOURCE) utm_source, coalesce(GOKWIK_UTM_medium,SHOPIFYQL_LAST_MOMENT_UTM_MEDIUM) utm_medium, coalesce(GOKWIK_UTM_campaign,SHOPIFYQL_LAST_VISIT_UTM_CAMPAIGN) utm_campaign, final_utm_channel, sum(total_sales) total_sales, COUNT(DISTINCT a.order_name) AS order_count from prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS a left join (select id, upper(GOKWIK_UTM_SOURCE)as GOKWIK_UTM_SOURCE , upper(GOKWIK_UTM_MEDIUM) as GOKWIK_UTM_MEDIUM, upper(gokwik_utm_campaign) as gokwik_utm_campaign, from prd_db.justherbs.dwh_GOKWIK_SOURCE) GS on a.order_id = gs.id group by 1,2,3,4,5 ;",
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
            