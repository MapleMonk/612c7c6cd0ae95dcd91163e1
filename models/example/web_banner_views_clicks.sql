{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.banner_funnel_data as ( select to_date(date,\'YYYYMMDD\') as ga_date, itempromotionname as banner_name, itemsviewedinpromotion as views, itemsclickedinpromotion as clicks, itempromotionclickthroughrate as ctr from snitch_db.maplemonk.WEB_BANNER_VIEWS_CLICKS where banner_name != \'(not set)\' );",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from snitch_db.MAPLEMONK.web_banner_views_clicks
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        