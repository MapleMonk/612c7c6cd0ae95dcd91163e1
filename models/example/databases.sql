{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.ga_full_funnel_view AS with overall_session_data as (select TO_DATE(DATE,\'YYYYMMDD\') AS GA_DATE, \'APP\' as TYPE, activeusers as Users, sessions as Sessions, engagedsessions as Engaged_Sessions, addtocarts as Add_To_Carts, checkouts as Checkouts, sessionsperuser as Sessions_Per_User from snitch_db.maplemonk.GA__FUNNEL__VIEW__TABLE_1 UNION select TO_DATE(DATE,\'YYYYMMDD\') AS GA_DATE, \'WEB\' as TYPE, activeusers as Users, sessions as Sessions, engagedsessions as Engaged_Sessions, addtocarts as Add_To_Carts, checkouts as Checkouts, sessionsperuser as Sessions_Per_User from snitch_db.maplemonk.WEB_GA_FUNNEL_VIEW_TABLE1 ), sales_data as (select order_timestamp::date as GA_DATE, CASE WHEN lower(webshopney) = \'appbrew\' THEN \'app\' ELSE webshopney end as TYPE, count(distinct order_name) as Order_Count from snitch_db.maplemonk.fact_items_snitch group by 1,2 ) select * from snitch_db.maplemonk.fact_items_snitch where order_timestamp::date = \'2024-03-10\' SELECT osd.ga_date, osd.type, osd.Users, osd.Sessions, osd.Engaged_Sessions, osd.Add_To_Carts, osd.Checkouts, osd.Sessions_Per_User, sd.Order_Count FROM overall_session_data osd LEFT JOIN sales_data sd ON osd.GA_DATE = sd.GA_DATE AND lower(osd.TYPE) = lower(sd.TYPE);",
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
                        