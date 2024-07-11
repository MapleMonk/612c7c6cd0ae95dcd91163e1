{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table bsc_db.maplemonk.bsc_db_pandl as select \'Sales\' type, brand, a.reference_code ordeR_name, marketplace, coalesce(a.channel, c.channel, d.channel) channel, source, case when marketplace = \'CRED\' then \'CRED\' else FINAL_UTM_CHANNEL_TYPE end FINAL_UTM_CHANNEL_TYPE, coalesce(a.order_Date, c.date, d.date) ordeR_date, coalesce(f.order_status,a.order_Status) ordeR_status, shipping_status, final_shipping_status, sku, product_name_final, product_sub_category, product_category, quantity, selling_price, b.mrp mrp_raw, ifnull(b.mrp,0)*quantity mrp, ifnull(b.mrp,0)*quantity - selling_price as mrp_discount, ifnull(b.cogs,0)*quantity cogs, case when lower(f.ordeR_status) like \'%return%\' then selling_price end returned_sales, 0.18*selling_price tax, div0(50,count(1) over (partition by order_name)) logistics_cost, div0(c.performance_marketing_spend, count(1) over (partition by ordeR_Date::Date, a.channel)) performance_marketing_spend, div0(d.brand_marketing_spend, count(1) over (partition by ordeR_Date::Date, a.channel)) brand_marketing_spend, case when lower(a.ordeR_Status) <> \'cancelled\' then e.commission_value*selling_price end as affiliate_commission, discount_affiliate, discount_Code, null return_amount, null return_quantity, null return_mrp, null return_tax from BSC_DB.MAPLEMONK.BSC_DB_sales_consolidated a left join ( select sku_code, replace(cogs,\',\',\'\') cogs, replace(mrp,\',\',\'\') mrp from ( select *, row_number() over (partition by sku_code order by 1) rw from bsc_db.maplemonk.sku_mrp_cogs ) where rw = 1) b on lower(a.sku) = lower(b.sku_code) left join (select date, channel,sum(spend) performance_marketing_spend from bsc_db.maplemonk.bsc_db_marketing_consolidated where campaign_type = \'PERFORMANCE\' group by 1,2) c on a.ordeR_date::Date = c.date and lower(a.channel) = lower(c.channel) left join (select date, channel,sum(spend) brand_marketing_spend from bsc_db.maplemonk.bsc_db_marketing_consolidated where campaign_type = \'BRAND\' group by 1,2) d on a.ordeR_date::Date = d.date and lower(a.channel) = lower(d.channel) left join (select affiliate_name,div0(replace(commission_value,\'%\',\'\'),100) commission_value from bsc_db.maplemonk.affiliate_commissions) e on upper(replace(a.source,\' \',\'\')) = upper(replace(e.affiliate_name,\' \',\'\')) left join (select distinct reference_code, SALEORDERITEMCODE, order_status, updated_date from bsc_db.MAPLEMONK.bsc_db_Vinculum_fact_items where lower(marketplace) like \'%shopify%\' and brand in (\'Bombay Shaving Company\')) f on a.reference_code = f.reference_code and a.SALEORDERITEMCODE = f.SALEORDERITEMCODE where a.brand = \'Bombay Shaving Company\' and a.marketplace = \'SHOPIFY\' union all select \'Sales\' as type, brand, a.reference_code ordeR_name, marketplace, coalesce(a.channel, c.channel, d.channel) channel, source, case when marketplace = \'CRED\' then \'CRED\' else FINAL_UTM_CHANNEL_TYPE end FINAL_UTM_CHANNEL_TYPE, coalesce(a.order_Date, c.date, d.date) ordeR_date, coalesce(f.order_status,a.order_Status) ordeR_status, shipping_status, final_shipping_status, sku, product_name_final, product_sub_category, product_category, quantity, selling_price, b.mrp mrp_raw, ifnull(b.mrp,0)*quantity mrp, ifnull(b.mrp,0)*quantity - selling_price as mrp_discount, ifnull(b.cogs,0)*quantity cogs, case when lower(f.ordeR_status) like \'%return%\' then selling_price end returned_sales, 0.18*selling_price tax, div0(50,count(1) over (partition by order_name)) logistics_cost, div0(c.performance_marketing_spend, count(1) over (partition by ordeR_Date::Date, a.channel)) performance_marketing_spend, div0(d.brand_marketing_spend, count(1) over (partition by ordeR_Date::Date, a.channel)) brand_marketing_spend, case when lower(a.ordeR_Status) <> \'cancelled\' then e.commission_value*selling_price end as affiliate_commission, discount_affiliate, discount_code, null return_amount, null return_quantity, null return_mrp, null return_tax from BSC_DB.MAPLEMONK.BSC_DB_sales_consolidated a left join ( select sku_code, replace(cogs,\',\',\'\') cogs, replace(mrp,\',\',\'\') mrp from ( select *, row_number() over (partition by sku_code order by 1) rw from bsc_db.maplemonk.sku_mrp_cogs ) where rw = 1) b on lower(a.sku) = lower(b.sku_code) left join (select date, channel,sum(spend) performance_marketing_spend from bsc_db.maplemonk.Bombae_MARKETING_CONSOLIDATED where campaign_type = \'PERFORMANCE\' group by 1,2) c on a.ordeR_date::Date = c.date and lower(a.channel) = lower(c.channel) left join (select date, channel,sum(spend) brand_marketing_spend from bsc_db.maplemonk.Bombae_MARKETING_CONSOLIDATED where campaign_type = \'BRAND\' group by 1,2) d on a.ordeR_date::Date = d.date and lower(a.channel) = lower(d.channel) left join (select affiliate_name,div0(replace(commission_value,\'%\',\'\'),100) commission_value from bsc_db.maplemonk.affiliate_commissions) e on upper(replace(a.source,\' \',\'\')) = upper(replace(e.affiliate_name,\' \',\'\')) left join (select distinct reference_code, SALEORDERITEMCODE, order_status, updated_date from bsc_db.MAPLEMONK.bsc_db_Vinculum_fact_items where lower(marketplace) like \'%shopify%\' and brand in (\'Bombae\')) f on a.reference_code = f.reference_code and a.SALEORDERITEMCODE = f.SALEORDERITEMCODE where a.brand = \'Bombae\' and a.marketplace = \'SHOPIFY\' union all select \'Return\' as type, case when lower(a.brand) = \'bombae\' then \'Bombae\' else a.brand end brand, a.reference_code ordeR_name, \'SHOPIFY\' marketplace, b.channel channel, b.source source, b.final_utm_channel_type FINAL_UTM_CHANNEL_TYPE, return_date ordeR_date, ordeR_status, ordeR_status shipping_status, ordeR_status final_shipping_status, sku, upper(coalesce(p.name, product_name)) product_name_final, upper(p.sub_Category) product_sub_category, upper(p.category) product_category, null quantity, null selling_price, null mrp_raw, null mrp, null as mrp_discount, null cogs, null returned_sales, null tax, div0(50,count(1) over (partition by order_name)) logistics_cost, null performance_marketing_spend, null brand_marketing_spend, null affiliate_commission, null discount_affiliate, null discount_Code, selling_price return_amount, return_quantity, return_quantity*mrp.mrp return_mrp, tax return_tax from bsc_db.MAPLEMONK.bsc_db_Vinculum_fact_items a left join (select * from (select skucode, productname name, category, sub_category, brand, sku_type, row_number() over (partition by skucode order by 1) rw from BSC_DB.MAPLEMONK.sku_master) where rw = 1 ) p on a.sku = p.skucode left join ( select sku_code, replace(cogs,\',\',\'\') cogs, replace(mrp,\',\',\'\') mrp from ( select *, row_number() over (partition by sku_code order by 1) rw from bsc_db.maplemonk.sku_mrp_cogs ) where rw = 1) mrp on lower(a.sku) = lower(mrp.sku_code) left join ( select * from (select reference_code, channel, source, final_utm_channel_type, row_number() over (partition by reference_code order by 1) rw from BSC_DB.MAPLEMONK.BSC_DB_sales_consolidated where marketplace = \'SHOPIFY\' )where rw=1 )b on a.reference_code = b.reference_code where lower(a.brand) in (\'bombay shaving company\',\'bombae\') and lower(a.marketplace) like \'%shopify%\' and a.return_date is not null",
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
                        