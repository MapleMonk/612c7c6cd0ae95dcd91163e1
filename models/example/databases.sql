{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.cac_online_overall as with online as ( select \'+91\' || RIGHT(REGEXP_REPLACE(phone, \'[^0-9]\', \'\'), 10) AS phone_no, min(order_timestamp::date) as online_min from snitch_db.maplemonk.fact_items_snitch group by 1 ), offline as ( select \'+91\' || RIGHT(REGEXP_REPLACE(phone, \'[^0-9]\', \'\'), 10) AS phone_no, min(order_date) as offline_min from snitch_db.maplemonk.STORE_fact_items_offline group by 1 ), acq_date as ( select coalesce(a.phone_no,b.phone_no) as phone, a.online_min, b.offline_min, case when online_min is null and offline_min is not null then offline_min when offline_min is null and online_min is not null then online_min else least(b.offline_min,a.online_min) end as overall_min from online a full outer join offline b on a.phone_no = b.phone_no ), acq_final as ( select *, case when online_min = overall_min then \'online_acq\' else \'offline_acq\' end as acq_channel from acq_date ), acq_month as ( select overall_min as acq_date, count(case when acq_channel = \'online_acq\' then phone end) as online_new_customers, count(phone) as overall_new_customers from acq_final group by 1 ), spends as ( select date, sum(spend) as spend from snitch_db.maplemonk.marketing_consolidated_snitch group by 1 ) select a.*, b.spend::int as spend from acq_month a left join spends b on a.acq_date = b.date",
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
            