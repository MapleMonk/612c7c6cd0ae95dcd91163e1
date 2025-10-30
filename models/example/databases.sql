{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.cac_online_overall as with online as ( select \'+91\' || RIGHT(REGEXP_REPLACE(phone, \'[^0-9]\', \'\'), 10) AS phone_no, min(order_timestamp::date) as online_min from snitch_db.maplemonk.fact_items_snitch group by 1 ), offline as ( select \'+91\' || RIGHT(REGEXP_REPLACE(phone, \'[^0-9]\', \'\'), 10) AS phone_no, min(order_date) as offline_min from snitch_db.maplemonk.STORE_fact_items_offline group by 1 ), acq_date as ( select coalesce(a.phone_no,b.phone_no) as phone, a.online_min, b.offline_min, case when online_min is null and offline_min is not null then offline_min when offline_min is null and online_min is not null then online_min else least(b.offline_min,a.online_min) end as overall_min from online a full outer join offline b on a.phone_no = b.phone_no ), acq_online as ( select online_min, count(phone) as online_customers from acq_date group by 1 ), acq_overall as ( select overall_min, count(phone) as overall_customers from acq_date group by 1 ), spends as ( select date, sum(spend) as spend from snitch_db.maplemonk.marketing_consolidated_snitch group by 1 ), cac_online as ( select a.date, a.spend, b.online_customers, div0(a.spend,b.online_customers)::int as online_cac from spends a left join acq_online b on a.date = b.online_min ), cac_overall as ( select a.date, round(a.spend,0) as spend, b.overall_customers, div0(a.spend,b.overall_customers)::int as overall_cac from spends a left join acq_overall b on a.date = b.overall_min ) select a.*, b.overall_customers, b.overall_cac from cac_online a left join cac_overall b on a.date = b.date",
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
            