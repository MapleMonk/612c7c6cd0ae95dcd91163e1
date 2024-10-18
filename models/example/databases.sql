{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table prd_db.justherbs.dwh_gross_contribution_JH as with additional_sku_mapping as ( select sku, model_no, ifnull(MRP,0) mrp, tax_rate::float GST, weight sku_weight, from datalake_db.justherbs.mst_easyecom_jh_product_master qualify row_number() over(partition by lower(model_no) order by 1) = 1 ) , orders as ( select * from prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS where (lower(order_status) <> \'cancelled\' or is_refund <> 1) ) select final_utm_channel final_utm_channel_nr, order_timestamp, order_id, ordeR_name, mrp_sales, total_sales, total_discount, div0(ifnull(total_discount,0),ifnull(mrp_sales,0)) discount_percent, NR, COGS total_COGS, 39 as logistics_cost, 21 as spm_cost, ifnull(NR,0) - ifnull(total_COGS,0) - ifnull(spm_cost,0) - ifnull(logistics_cost,0) as GC, div0(GC,NR) GC_percent, discount_code, product_list, GC_percent*100 GC_filter, order_timestamp::string timestamped_date, from ( select order_timestamp, final_utm_channel, order_id, ordeR_name, discount_code, listagg(product_name, \', \') product_list, sum(ifnull(o.total_sales,0)) total_Sales, sum(ifnull(b.mrp,0)*ifnull(o.quantity,0)) mrp_sales, sum(ifnull(b.mrp,0)*ifnull(o.quantity,0) - (ifnull(o.GROSS_SALES_AFTER_TAX,0) - ifnull(discount,0))) total_discount, sum(div0((ifnull(o.GROSS_SALES_AFTER_TAX,0) - ifnull(discount,0)),(1+ifnull(asm.gst,0))) + div0(ifnull(o.shipping_price,0),1.18)) NR, ifnull(sum(ifnull(b.cogs,0)*ifnull(o.quantity,0)),sum(ifnull(b.mrp,0)*ifnull(o.quantity,0))*0.15) COGS, sum(ifnull(o.quantity,0)) quantity, sum(ifnull(asm.sku_weight,0)*ifnull(o.quantity,0)) order_weight from orders o left join additional_sku_mapping asm on o.sku = asm.model_no left join (select * from (select sku_code, start_Date, end_date, replace(mrp,\',\',\'\')::float mrp, replace(cogs,\',\',\'\')::float cogs, row_number() over (partition by lower(sku_code), start_date, end_Date order by 1) rw from datalake_db.justherbs.mst_sku_mrp_cogs) where rw = 1 ) b on lower(o.sku) = lower(b.sku_code) and o.ordeR_timestamp::date >= b.start_Date and o.order_timestamp::date <= b.end_Date group by 1,2,3,4,5 ) ;",
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
            