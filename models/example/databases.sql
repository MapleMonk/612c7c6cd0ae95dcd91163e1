{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.ga_full_funnel_view AS with overall_session_data as ( select TO_DATE(DATE,\'YYYYMMDD\') AS Date, \'app\' as Type, activeusers as Users, sessions as Sessions, engagedsessions as Engaged_Sessions, addtocarts as Add_To_Carts, checkouts as Checkouts, sessionsperuser as Sessions_Per_User from snitch_db.maplemonk.GA__FUNNEL__VIEW__TABLE_1 UNION select TO_DATE(DATE,\'YYYYMMDD\') AS Date, \'web\' as Type, activeusers as Users, sessions as Sessions, engagedsessions as Engaged_Sessions, addtocarts as Add_To_Carts, checkouts as Checkouts, sessionsperuser as Sessions_Per_User from snitch_db.maplemonk.WEB_GA_FUNNEL_VIEW_TABLE1 ), clicks_impression_data as ( SELECT TO_DATE(DATE,\'YYYYMMDD\') AS Date, \'web\' AS Type, itemlistviewevents AS Impressions, itemviewevents AS Clicks, FROM snitch_db.maplemonk.WEB_GA_FUNNEL_VIEW_WEB_TABLE3 UNION SELECT TO_DATE(DATE,\'YYYYMMDD\') AS Date, \'app\' AS Type, itemlistviewevents AS Impressions, itemviewevents AS Clicks, FROM snitch_db.maplemonk.GA_FUNNEL_VIEW_TABLE2 ), sales_data as ( select order_timestamp::date as Date, CASE WHEN lower(webshopney) = \'appbrew\' THEN \'app\' ELSE webshopney end as type, count(distinct order_name) as Order_Count from snitch_db.maplemonk.fact_items_snitch group by 1,2 ) SELECT osd.Date, osd.Type, osd.Users, osd.Sessions, osd.Engaged_Sessions, cid.Impressions, cid.Clicks, osd.Add_To_Carts, osd.Checkouts, osd.Sessions_Per_User, sd.Order_Count FROM overall_session_data osd LEFT JOIN sales_data sd ON osd.Date = sd.Date AND lower(osd.Type) = lower(sd.Type) left join clicks_impression_data cid on osd.Date = cid.Date and lower(osd.Type) = lower(cid.Type)",
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
                        