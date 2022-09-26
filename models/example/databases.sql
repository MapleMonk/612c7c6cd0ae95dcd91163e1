{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table xyxx_db.maplemonk.Contlo_Fact_Items_XYXX as select id ,name ,\'Campaigns\' as Type ,CHANNEL ,\'Published\' as Status ,NULL as Action_ID ,try_to_date(sent_time,\'yyyy-mm-dd HH:MI:SS +0530\') Sent_Date ,try_to_date(updated_at,\'yyyy-mm-ddTHH:MI:SS.FF3+05:30\') Updated_Date ,replace(revenue,\'₹\',\'\')::float Revenue ,SENT_COUNT ,OPENED_COUNT ,CLICKED_COUNT ,ORDERS_COUNT ,BOUNCED_COUNT ,UNSUBSCRIBED_COUNT ,NULL as COMPLAINED_COUNT from xyxx_db.maplemonk.contlo_campaigns union all select id ,name ,\'Automation\' as Type ,replace(A.Value:type,\'\"\',\'\') CHANNEL ,status ,A.Value:id Action_ID ,try_to_date(published_date) PUBLISHED_DATE ,try_to_date(replace(A.Value:updated_at,\'\"\',\'\'),\'yyyy-mm-ddTHH:MI:SS.FF3+05:30\') Updated_Date ,A.Value:total_earned::float Action_Revenue ,A.Value:sent_count::float Action_Sent ,A.Value:opened_count::float Action_Opens ,A.Value:clicked_count::float Action_Clicks ,A.Value:orders_count::float Action_Orders ,A.Value:bounced_count::float Action_Bounced ,A.Value:unsubscribed_count::float Action_Unsubscriptions ,A.Value:complained_count::float Action_Complaints from xyxx_db.maplemonk.contlo_automation, LATERAL FLATTEN (INPUT => ACTION_SUMMARY)A; create or replace table xyxx_db.maplemonk.Contlo_Fact_Items_XYXX as select a.* ,b.cost per_action_cost ,a.sent_count*b.cost total_cost from xyxx_db.maplemonk.Contlo_Fact_Items_XYXX a left join retention_spend_mapping b on lower(a.channel) = lower(b.type); CREATE OR REPLACE table XYXX_DB.MAPLEMONK.Contlo_Fact_Items_XYXX AS with cte as ( select order_timestamp::date as Date,final_utm_channel,lower(coalesce(landing_utm_medium,referring_utm_medium,\'others\')) as final_utm_medium,sum(total_sales) as Shopify_revenue from fact_items_shopify_xyxx where lower(final_utm_channel) = \'contlo\' and lower(coalesce(landing_utm_medium,referring_utm_medium,\'others\')) in (\'sms\',\'bpn\',\'whatsapp\',\'email\') group by 1,final_utm_channel,3 order by 1 desc) select a.ID, a.NAME, a.TYPE, a.CHANNEL, a.STATUS, a.ACTION_ID, a.SENT_DATE, a.UPDATED_DATE, a.REVENUE, a.SENT_COUNT, a.OPENED_COUNT, a.CLICKED_COUNT, a.ORDERS_COUNT, a.BOUNCED_COUNT, a.UNSUBSCRIBED_COUNT, a.COMPLAINED_COUNT, a.PER_ACTION_COST::float as PER_ACTION_COST, a.TOTAL_COST, c.shopify_revenue/count(1) over(partition by a.sent_date,a.channel) as shopify_revenue from xyxx_db.maplemonk.contlo_fact_items_xyxx a left join cte c on a.sent_date=c.date and lower((case when lower(a.channel) <> \'web push\' then a.channel else \'bpn\' end))= lower(c.final_utm_medium)",
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
                        