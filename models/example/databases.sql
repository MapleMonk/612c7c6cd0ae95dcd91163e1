{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table prd_db.justherbs.dwh_missing_sku_mrp_cogs_mappings as select distinct a.sku from prd_db.justherbs.dwh_gross_contribution_JH a left join (select * from (select sku_code, start_Date, end_date, case when replace(replace(cogs,\',\',\'\'),\'-\',\'\') = \'\' then 0 else replace(replace(cogs,\',\',\'\'),\'-\',\'\')::float end cogs, row_number() over (partition by lower(sku_code), start_date, end_Date order by 1) rw from datalake_db.justherbs.mst_sku_mrp_cogs) where rw = 1 ) b on lower(a.sku) = lower(b.sku_code) and a.ordeR_timestamp::date >= b.start_Date and a.order_timestamp::date <= b.end_Date where b.cogs is null ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from PRD_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            