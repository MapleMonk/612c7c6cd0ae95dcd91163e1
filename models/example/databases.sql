{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table bsc_db.maplemonk.bsc_db_affiliate_validation as select \'Bombay Shaving Company\' as brand ,order_timestamp::date ordeR_Date ,order_name ,utm_source ,coalesce(b.ordeR_status,a.order_status) status ,discount_code ,case when lower(a.shopify_new_customer_flag) = \'new\' then \'New\' else \'Not New\' end new_Customer_flag ,c.channel discount_code_channel ,case when lower(c.channel) not in (\'alliance\', \'b2b\') and lower(a.shopify_new_customer_flag) = \'new\' and lower(coalesce(b.ordeR_status,a.order_status)) = \'delivered\' then \'Yes\' when lower(c.channel) = (\'alliance\') and lower(a.shopify_new_customer_flag) = \'new\' and lower(coalesce(b.ordeR_status,a.order_status)) = \'delivered\' then \'Alliance Code Used\' else \'No\' end as payout_validation ,sum(total_sales) total_Sales from bsc_db.maplemonk.bsc_db_shopify_fact_items a left join bsc_db.MAPLEMONK.bsc_db_Vinculum_fact_items b on a.ordeR_name = b.reference_code and a.line_item_id = b.SALEORDERITEMCODE left join (select * from ( select *, row_number() over (partition by code_prefix order by 1) rw from BSC_DB.maplemonk.mapping_code_affiliate)where rw = 1 )c on upper(a.discount_code) LIKE concat(c.code_prefix,\'%\') group by 1,2,3,4,5,6,7,8,9 union all select \'Bombae\' as Brand ,order_timestamp::date ordeR_Date ,order_name ,utm_source ,coalesce(b.ordeR_status,a.order_status) status ,discount_code ,case when lower(a.shopify_new_customer_flag) = \'new\' then \'New\' else \'Not New\' end new_Customer_flag ,c.channel discount_code_channel ,case when lower(c.channel) not in (\'alliance\', \'b2b\') and lower(a.shopify_new_customer_flag) = \'new\' and lower(coalesce(b.ordeR_status,a.order_status)) = \'delivered\' then \'Yes\' when lower(c.channel) = (\'alliance\') and lower(a.shopify_new_customer_flag) = \'new\' and lower(coalesce(b.ordeR_status,a.order_status)) = \'delivered\' then \'Alliance Code Used\' else \'No\' end as payout_validation ,sum(total_sales) total_Sales from BSC_DB.MAPLEMONK.Bombae_SHOPIFY_FACT_ITEMS a left join bsc_db.MAPLEMONK.bsc_db_Vinculum_fact_items b on a.ordeR_name = b.reference_code and a.line_item_id = b.SALEORDERITEMCODE left join (select * from ( select *, row_number() over (partition by code_prefix order by 1) rw from BSC_DB.maplemonk.mapping_code_affiliate)where rw = 1 )c on upper(a.discount_code) LIKE concat(c.code_prefix,\'%\') group by 1,2,3,4,5,6,7,8,9 ;",
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
                        