{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.web_app_conversion_snitch as select a.date, web_sessions, web_orders, div0(ifnull(web_orders,0),ifnull(web_sessions,0)) web_conversion, shopney_orders, shopney_sessions, div0(ifnull(shopney_orders,0),ifnull(shopney_sessions,0)) shopney_conversion, appbrew_sessions, appbrew_orders, div0(ifnull(appbrew_orders,0),ifnull(appbrew_sessions,0)) appbrew_conversion, ifnull(web_sessions,0) + ifnull(shopney_sessions,0) + ifnull(appbrew_sessions,0) as total_sessions, ifnull(web_orders,0) + ifnull(shopney_orders,0) + ifnull(appbrew_orders,0) as total_orders from ( select to_date(date, \'YYYYMMDD\') date, sum(sessions) web_sessions from snitch_db.maplemonk.ga4_web_sessions_by_date where to_date(date, \'YYYYMMDD\') <=\'2024-02-27\' group by 1 union select to_date(date, \'YYYYMMDD\') date, sum(sessions) web_sessions from snitch_db.maplemonk.session_by_date_new_source where to_date(date, \'YYYYMMDD\') > \'2024-02-27\' group by 1 )a left join ( select order_timestamp::date date, count(distinct order_id) web_orders from snitch_db.maplemonk.fact_items_snitch where webshopney = \'Web\' group by 1 )b on a.date = b.date left join ( select to_date(date, \'YYYYMMDD\') date, sum(sessions) shopney_sessions from snitch_db.maplemonk.ga4_snitch_mobile_app_sessions_by_date group by 1 )c on a.date = c.date left join ( select order_timestamp::date date, count(distinct order_id) shopney_orders from snitch_db.maplemonk.fact_items_snitch where webshopney = \'Shopney\' group by 1 )d on a.date = d.date left join ( select to_date(date, \'YYYYMMDD\') date, sum(sessions) appbrew_sessions from snitch_db.maplemonk.ga4_appbrew_sessions_by_date where lower(platform) in (\'ios\',\'android\') group by 1 )e on a.date = e.date left join ( select order_timestamp::date date, count(distinct order_id) Appbrew_orders from snitch_db.maplemonk.fact_items_snitch where webshopney = \'Appbrew\' group by 1 )f on a.date = f.date ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from snitch_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        