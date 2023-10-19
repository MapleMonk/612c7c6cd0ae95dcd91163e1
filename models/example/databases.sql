{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table bsc_db.maplemonk.bsc_db_pandl as select reference_code ordeR_name, marketplace, a.channel, source, FINAL_UTM_CHANNEL_TYPE, order_Date, order_status, shipping_status, final_shipping_status, sku, product_name_final, product_sub_category, product_category, quantity, selling_price, ifnull(b.mrp,0)*quantity mrp, ifnull(b.mrp,0)*quantity - selling_price as mrp_discount, ifnull(b.cogs,0)*quantity cogs, returned_sales, 0.18*selling_price tax, div0(44,count(1) over (partition by order_name)) logistics_cost, div0(c.performance_marketing_spend, count(1) over (partition by ordeR_Date::Date, a.channel)) performance_marketing_spend, div0(d.brand_marketing_spend, count(1) over (partition by ordeR_Date::Date, a.channel)) brand_marketing_spend, case when lower(a.ordeR_Status) <> \'cancelled\' then e.commission_value*selling_price end as affiliate_commission, discount_affiliate from BSC_DB.MAPLEMONK.BSC_DB_sales_consolidated a left join ( select sku_code, replace(cogs,\',\',\'\') cogs, replace(mrp,\',\',\'\') mrp from ( select *, row_number() over (partition by sku_code order by 1) rw from bsc_db.maplemonk.sku_mrp_cogs ) where rw = 1) b on a.sku = b.sku_code left join (select date, channel,sum(spend) performance_marketing_spend from bsc_db.maplemonk.bsc_db_marketing_consolidated where campaign_type = \'PERFORMANCE\' group by 1,2) c on a.ordeR_date::Date = c.date and lower(a.channel) = lower(c.channel) left join (select date, channel,sum(spend) brand_marketing_spend from bsc_db.maplemonk.bsc_db_marketing_consolidated where campaign_type = \'BRAND\' group by 1,2) d on a.ordeR_date::Date = d.date and lower(a.channel) = lower(d.channel) left join (select affiliate_name,div0(replace(commission_value,\'%\',\'\'),100) commission_value from bsc_db.maplemonk.affiliate_commissions) e on upper(replace(a.source,\' \',\'\')) = upper(replace(e.affiliate_name,\' \',\'\'))",
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
                        