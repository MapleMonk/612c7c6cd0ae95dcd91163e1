{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table prd_db.justherbs.dwh_gross_contribution_JH as with additional_sku_mapping as ( select sku, model_no, ifnull(MRP,0) mrp, tax_rate::float GST, weight sku_weight, from datalake_db.justherbs.mst_easyecom_jh_product_master ) , orders as ( select * from prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS where (lower(order_status) <> \'cancelled\' or is_refund <> 1) ) select order_timestamp, order_id, ordeR_name, mrp_sales, total_sales, total_discount, div0(total_discount,mrp_sales) discount_percent, NR, COGS total_COGS, 39 as logistics_cost, 21 as spm_cost, NR - total_COGS - spm_cost - logistics_cost as GC, div0(GC,NR) GC_percent, discount_code, product_list, GC_percent*100 GC_filter, order_timestamp::string timestamped_date, from ( select order_timestamp, order_id, ordeR_name, discount_code, listagg(product_name, \', \') product_list, sum(o.total_sales) total_Sales, sum(b.mrp*o.quantity) mrp_sales, sum(b.mrp*o.quantity - (o.GROSS_SALES_AFTER_TAX - discount)) total_discount, sum(div0((o.GROSS_SALES_AFTER_TAX - discount),(1+asm.gst)) + div0(o.shipping_price,1.18)) NR, ifnull(sum(b.cogs*o.quantity),sum(b.mrp*o.quantity)*0.15) COGS, sum(o.quantity) quantity, sum(asm.sku_weight*o.quantity) order_weight from orders o left join additional_sku_mapping asm on o.sku = asm.model_no left join (select * from (select sku_code, start_Date, end_date, replace(mrp,\',\',\'\')::float mrp, replace(cogs,\',\',\'\')::float cogs, row_number() over (partition by sku_code, start_date, end_Date order by 1) rw from datalake_db.justherbs.mst_sku_mrp_cogs) where rw = 1 ) b on lower(o.sku) = lower(b.sku_code) and o.ordeR_timestamp::date >= b.start_Date and o.order_timestamp::date <= b.end_Date group by 1,2,3,4 ) ;",
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
            