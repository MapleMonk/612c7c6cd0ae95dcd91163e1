{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.ga_full_funnel_view AS with overall_session_data as ( select TO_DATE(DATE,\'YYYYMMDD\') AS Date, \'app\' as Type, activeusers as Users, sessions as Sessions, engagedsessions as Engaged_Sessions, addtocarts as Add_To_Carts, checkouts as Checkouts, sessionsperuser as Sessions_Per_User from snitch_db.maplemonk.GA__FUNNEL__VIEW__TABLE_1 UNION select TO_DATE(DATE,\'YYYYMMDD\') AS Date, \'web\' as Type, activeusers as Users, sessions as Sessions, engagedsessions as Engaged_Sessions, addtocarts as Add_To_Carts, checkouts as Checkouts, sessionsperuser as Sessions_Per_User from snitch_db.maplemonk.WEB_GA_FUNNEL_VIEW_TABLE1 ), clicks_impression_data as ( SELECT TO_DATE(DATE,\'YYYYMMDD\') AS Date, \'web\' AS Type, itemlistviewevents AS Impressions, itemviewevents AS Clicks, FROM snitch_db.maplemonk.WEB_GA_FUNNEL_VIEW_WEB_TABLE3 UNION SELECT TO_DATE(DATE,\'YYYYMMDD\') AS Date, \'app\' AS Type, itemlistviewevents AS Impressions, itemviewevents AS Clicks, FROM snitch_db.maplemonk.GA_FUNNEL_VIEW_TABLE2 ), sales_data as ( select order_timestamp::date as Date, CASE WHEN lower(webshopney) = \'appbrew\' THEN \'app\' ELSE webshopney end as type, count(distinct order_name) as Order_Count from snitch_db.maplemonk.fact_items_snitch where lower(ifnull(discount_code,\'n\')) not like \'%eco%\' and lower(ifnull(discount_code,\'n\')) not like \'%influ%\' and order_name not in (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') and tags not in (\'FLITS_LOGICERP\') and order_timestamp::date > \'2024-04-10\' group by 1,2 ) SELECT osd.Date, osd.Type, osd.Users, osd.Sessions, osd.Engaged_Sessions, cid.Impressions, cid.Clicks, osd.Add_To_Carts, osd.Checkouts, osd.Sessions_Per_User, sd.Order_Count FROM overall_session_data osd LEFT JOIN sales_data sd ON osd.Date = sd.Date AND lower(osd.Type) = lower(sd.Type) left join clicks_impression_data cid on osd.Date = cid.Date and lower(osd.Type) = lower(cid.Type)",
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
                        