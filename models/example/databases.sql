{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table bsc_db.maplemonk.bsc_db_producT_combination as select first_sku, rw, sku, first_order_month,customers, div0(customers,sum(customers) over (partition by first_sku, rw,first_ordeR_month)) customer_share from ( select \'SHAVE_SENSI_SMART-3_RAZOR_GREEN_149\' as first_sku, rw, sku,date_trunc(\'month\',first_order_date) first_order_month, count(customer_id_final) customers from (select a.customer_id_final, a.ordeR_Date::Date ordeR_date, b.first_order_date, a.sku, row_number() over (partition by a.customer_id_final order by order_date::date) rw from BSC_DB.MAPLEMONK.BSC_DB_sales_consolidated a left join (select customer_id_final, min(ordeR_Date::Date) first_ordeR_Date from BSC_DB.MAPLEMONK.BSC_DB_sales_consolidated where sku in (\'SHAVE_SENSI_SMART-3_RAZOR_GREEN_149\') and lower(marketplace) like \'%shopify%\' group by 1) b on a.customer_id_final = b.customer_id_final where a.order_date > b.first_ordeR_Date and lower(a.marketplace) like \'%shopify%\' ) group by 1,2,3,4 union all select \'SHAVE_SENSIFLO-4_RAZOR\' as first_sku, rw, sku,date_trunc(\'month\',first_order_date) first_order_month, count(customer_id_final) customers from (select a.customer_id_final, a.ordeR_Date::Date ordeR_date, b.first_order_date, a.sku, row_number() over (partition by a.customer_id_final order by order_date::date) rw from BSC_DB.MAPLEMONK.BSC_DB_sales_consolidated a left join (select customer_id_final, min(ordeR_Date::Date) first_ordeR_Date from BSC_DB.MAPLEMONK.BSC_DB_sales_consolidated where sku in (\'SHAVE_SENSIFLO-4_RAZOR\') and lower(marketplace) like \'%shopify%\' group by 1) b on a.customer_id_final = b.customer_id_final where a.order_date > b.first_ordeR_Date and lower(a.marketplace) like \'%shopify%\' ) group by 1,2,3,4 union all select \'SHAVE_SENSIFLO-6_RAZOR\' as first_sku, rw, sku,date_trunc(\'month\',first_order_date) first_order_month, count(customer_id_final) customers from (select a.customer_id_final, a.ordeR_Date::Date ordeR_date, b.first_order_date, a.sku, row_number() over (partition by a.customer_id_final order by order_date::date) rw from BSC_DB.MAPLEMONK.BSC_DB_sales_consolidated a left join (select customer_id_final, min(ordeR_Date::Date) first_ordeR_Date from BSC_DB.MAPLEMONK.BSC_DB_sales_consolidated where sku in (\'SHAVE_SENSIFLO-6_RAZOR\') and lower(marketplace) like \'%shopify%\' group by 1) b on a.customer_id_final = b.customer_id_final where a.order_date > b.first_ordeR_Date and lower(a.marketplace) like \'%shopify%\' ) group by 1,2,3,4 );",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from BSC_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        