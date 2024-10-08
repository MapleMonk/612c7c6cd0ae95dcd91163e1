{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.ticket_rollover as with tickets_raised as ( select initiated_at::date as date, COUNT(DISTINCT CASE WHEN BOT_CONVERSATION_FLAG = \'No\' THEN interaction_id END) as tickets_issued from snitch_db.maplemonk.freshchat_frontent_old group by 1 ), tickets_resolved as ( select try_to_date(resolved_at::date) as date, COUNT(DISTINCT CASE WHEN BOT_CONVERSATION_FLAG = \'No\' AND RESPONSE_DUE_TYPE = \'NO_RESPONSE_DUE\' AND RESOLVED_AT IS NOT NULL THEN interaction_id END) as tickets_resolved from snitch_db.maplemonk.freshchat_frontent_old where resolved_at IS NOT NULL and resolved_at != \'\' group by 1 ), main_data as ( select a.*, b.tickets_resolved, a.tickets_issued-b.tickets_resolved as carry_forward from tickets_raised a left join tickets_resolved b on a.date = b.date ) select *, sum(carry_forward) over (order by date asc) as cumulative_carry_forward from main_data",
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
            