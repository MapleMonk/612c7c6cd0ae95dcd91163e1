{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table xyxx_db.maplemonk.Contlo_Fact_Items_XYXX as select id ,name ,\'Campaigns\' as Type ,CHANNEL ,\'Published\' as Status ,NULL as Action_ID ,try_to_date(sent_time,\'yyyy-mm-dd HH:MI:SS +0530\') Sent_Date ,try_to_date(updated_at,\'yyyy-mm-ddTHH:MI:SS.FF3+05:30\') Updated_Date ,replace(revenue,\'₹\',\'\')::float Revenue ,SENT_COUNT ,OPENED_COUNT ,CLICKED_COUNT ,ORDERS_COUNT ,BOUNCED_COUNT ,UNSUBSCRIBED_COUNT ,NULL as COMPLAINED_COUNT from xyxx_db.maplemonk.contlo_campaigns union all select id ,name ,\'Automation\' as Type ,replace(A.Value:type,\'\"\',\'\') CHANNEL ,status ,A.Value:id Action_ID ,try_to_date(published_date) PUBLISHED_DATE ,try_to_date(replace(A.Value:updated_at,\'\"\',\'\'),\'yyyy-mm-ddTHH:MI:SS.FF3+05:30\') Updated_Date ,A.Value:total_earned::float Action_Revenue ,A.Value:sent_count::float Action_Sent ,A.Value:opened_count::float Action_Opens ,A.Value:clicked_count::float Action_Clicks ,A.Value:orders_count::float Action_Orders ,A.Value:bounced_count::float Action_Bounced ,A.Value:unsubscribed_count::float Action_Unsubscriptions ,A.Value:complained_count::float Action_Complaints from xyxx_db.maplemonk.contlo_automation, LATERAL FLATTEN (INPUT => ACTION_SUMMARY)A;",
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
                        