{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table bsc_db.maplemonk.bsc_db_affiliate_validation as select m.* , case when n.sku is null then \'Others\' else \'Trim/Razor\' end as mapped_category , o.commission commission_percentage , (replace(o.commission,\'%\',\'\')/100)*m.total_sales sales_commission from (select brand, ordeR_Date, order_name, utm_source, source,status, discount_code, new_customer_flag, discount_code_channel,payout_validation, category, sku, sum(total_sales) total_sales from ( select distinct \'Bombay Shaving Company\' as brand ,category ,a.sku ,order_timestamp::date ordeR_Date ,order_name ,line_item_id ,utm_source ,coalesce(b.ordeR_status,a.order_status) status ,discount_code ,case when lower(a.shopify_new_customer_flag) = \'new\' then \'New\' else \'Repeat\' end new_Customer_flag ,c.channel discount_code_channel ,coalesce(c.source, utm_source) source ,case when (lower(c.channel) not in (\'alliance\', \'b2b\') or lower(c.channel) is null) and lower(coalesce(b.ordeR_status,a.order_status)) = \'delivered\' then \'Yes\' when lower(c.channel) = (\'alliance\') and lower(coalesce(b.ordeR_status,a.order_status)) = \'delivered\' then \'Alliance Code Used\' else \'No\' end as payout_validation ,total_Sales from bsc_db.maplemonk.bsc_db_shopify_fact_items a left join bsc_db.MAPLEMONK.bsc_db_Vinculum_fact_items b on a.ordeR_name = b.reference_code and a.line_item_id = b.SALEORDERITEMCODE left join (select * from ( select *, row_number() over (partition by code_prefix order by 1) rw from BSC_DB.maplemonk.mapping_code_affiliate)where rw = 1 )c on upper(a.discount_code) LIKE concat(c.code_prefix,\'%\') ) group by 1,2,3,4,5,6,7,8,9,10,11,12 union all select brand, ordeR_Date, order_name, utm_source, source, status, discount_code, new_customer_flag, discount_code_channel,payout_validation, category, sku, sum(total_sales) total_sales from ( select distinct \'Bombae\' as Brand ,category ,a.sku ,order_timestamp::date ordeR_Date ,order_name ,line_item_id ,utm_source ,coalesce(b.ordeR_status,a.order_status) status ,discount_code ,case when lower(a.shopify_new_customer_flag) = \'new\' then \'New\' else \'Repeat\' end new_Customer_flag ,c.channel discount_code_channel ,coalesce(c.source, utm_source) source ,case when (lower(c.channel) not in (\'alliance\', \'b2b\') or lower(c.channel) is null) and lower(coalesce(b.ordeR_status,a.order_status)) = \'delivered\' then \'Yes\' when lower(c.channel) = (\'alliance\') and lower(coalesce(b.ordeR_status,a.order_status)) = \'delivered\' then \'Alliance Code Used\' else \'No\' end as payout_validation ,total_Sales from BSC_DB.MAPLEMONK.Bombae_SHOPIFY_FACT_ITEMS a left join bsc_db.MAPLEMONK.bsc_db_Vinculum_fact_items b on a.ordeR_name = b.reference_code and a.line_item_id = b.SALEORDERITEMCODE left join (select * from ( select *, row_number() over (partition by code_prefix order by 1) rw from BSC_DB.maplemonk.mapping_code_affiliate)where rw = 1 )c on upper(a.discount_code) LIKE concat(c.code_prefix,\'%\') ) group by 1,2,3,4,5,6,7,8,9,10,11,12 ) m left join (select distinct sku from bsc_db.maplemonk.trimmer_razor) n on upper(m.sku) = upper(n.sku) left join (select distinct partner, customer_type, category, commission from bsc_db.maplemonk.affiliate_commission) o on upper(o.partner) = upper(m.source) and upper(o.customer_type) = upper(m.new_customer_flag) and upper(o.category) = upper(case when n.sku is null then \'Others\' else \'Trim/Razor\' end) ;",
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
                        