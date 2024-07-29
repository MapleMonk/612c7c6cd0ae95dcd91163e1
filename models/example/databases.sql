{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.total_sales_dod as ( select date, \'Shopify\' as type, sum(gross_sales) as total_sales from snitch_db.maplemonk.sales_cost_source_snitch group by 1,2 union select order_date as date, marketplace_mapped as type, sum(case when lower(marketplace_mapped) = \'ajio\' then selling_price*1.45 else selling_price end) as total_sales from snitch_db.maplemonk.unicommerce_fact_items_snitch where marketplace_mapped IN (\'Myntra\',\'SNAPMINT\',\'CRED\',\'DONOSHOP\',\'FYND\',\'AMAZON\',\'MYNTRA\',\'MENSXP\',\'NYKAA_FASHION\',\'AJIO\',\'FLIPKART\') group by 1,2 union select date as date, marketplace_mapped as type, sum(today_sales) as total_sales from snitch_db.maplemonk.offline_detailed_summary group by 1,2 );",
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
            