{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table XYXX_DB.MAPLEMONK.SHOPIFY_FUNNEL_BY_PAGE_FACTITEMS as select try_to_date(DAY, \'yyyy-mm-dd\') DATE ,PAGE_PATH ,PAGE_TYPE ,UTM_CAMPAIGN_MEDIUM ,website.UTM_CAMPAIGN_SOURCE ,UTM_CAMPAIGN_CONTENT ,UTM_CAMPAIGN_NAME ,UTM_CAMPAIGN_TERM ,try_cast(TOTAL_VISITORS as float) TOTAL_VISITORS ,try_cast(TOTAL_CARTS as float) TOTAL_CARTS ,try_cast(TOTAL_CHECKOUTS as float) TOTAL_CHECKOUTS ,try_cast(TOTAL_ORDERS_PLACED as float) TOTAL_ORDERS_PLACED ,_AB_SOURCE_FILE_LAST_MODIFIED LAST_UPDATED ,_AB_SOURCE_FILE_URL SOURCE_URL ,case when channel is null then \'Direct\' else channel end as channel from ( select * from (select * ,row_number() over(partition by lower(utm_campaign_source), lower(utm_campaign_medium), lower(utm_campaign_name), lower(utm_campaign_content), lower(utm_campaign_term), lower(day), lower(page_path), lower(page_type) order by 1 )rw from xyxx_db.maplemonk.s3_website_funnel_data_page_path ) where rw=1 )website left join (select * from (select * , row_number() over (partition by lower(utm_campaign_source) order by 1) rw from xyxx_db.maplemonk.xyxx_utm_campaign_mapping) where rw=1 and utm_campaign_source is not null ) UTM_MAPPING on lower(website.utm_campaign_source) = lower(UTM_MAPPING.utm_campaign_source) ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from XYXX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        