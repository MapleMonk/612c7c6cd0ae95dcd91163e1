{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.nykaa_sales_growth_report_fact_items as with all_skus as ( select distinct upper(nullif(master_sku,\'\')) as sku from maplemonk.thd_pharma_nykaa_sales_fact_items union all select distinct nullif(replace(a_c_sku, \'`\', \'\'),\'\') as sku from maplemonk.easyecom_pharma_tax_sales where upper(mp_name) = \'NYKAA B2B OUTRIGHT\' ), calendar_dates as ( select day as date from unnest(generate_date_array(\'2026-01-01\', current_date(), interval 1 day)) as day ), all_sku_dates as ( select d.date, s.sku from calendar_dates d cross join all_skus s ), primary_sales as ( select date,sku, sum(ifnull(primary_item_quantity,0)) as primary_item_quantity, sum(ifnull(primary_selling_price,0)) as primary_selling_price, sum(ifnull(primary_taxable_value,0)) as primary_taxable_value from ( select date(invoice_date) as date, replace(a_c_sku,\'`\',\'\') as sku, sum(ifnull(cast(item_quantity as int64),0)) as primary_item_quantity, sum(ifnull(cast(selling_price as float64),0)) as primary_selling_price, sum(ifnull(cast(taxable_value as float64),0)) as primary_taxable_value from maplemonk.easyecom_pharma_tax_sales where upper(mp_name) = \'NYKAA B2B OUTRIGHT\' group by 1,2 union all select date(invoice_date) as date, replace(Parent_SKU,\'`\',\'\') as sku, -1 * sum(ifnull(cast(item_quantity as int64),0)) as primary_item_quantity, -1 * sum(ifnull(cast(selling_price as float64),0)) as primary_selling_price, -1 * sum(ifnull(cast(taxable_value as float64),0)) as primary_taxable_value from maplemonk.easyecom_pharma_tax_returns where upper(mp_name) = \'NYKAA B2B OUTRIGHT\' group by 1,2 ) group by 1,2 ), secondary_sales as ( select date(order_date) as date, master_sku, avg(ifnull(mrp,0)) as mrp, sum(ifnull(quantity,0)) as secondary_quantity, sum(ifnull(selling_price,0)) as selling_price, from maplemonk.thd_pharma_nykaa_sales_fact_items group by 1,2 ) select a.sku, pm.nykaa_sku, a.date, upper(pm.brand) as brand, pm.asp, pm.mrp as mrp, p.primary_item_quantity, p.primary_selling_price, p.primary_taxable_value, s.secondary_quantity, s.selling_price from all_sku_dates a left join primary_sales p on a.sku = p.sku and a.date = p.date left join secondary_sales s on a.sku = s.master_sku and a.date = s.date left join (select upper(trim(nykaa)) nykaa_sku, upper(trim(sku)) as master_sku, cast(mrp as float64) as mrp, cast(Avg_Selling_Price as float64) as asp, upper(trim(sub_category)) as sub_category, brand from maplemonk.inventory_product_MASTER where upper(trim(nykaa)) is not null qualify row_number() over (partition by upper(trim(sku)) order by upper(trim(nykaa)) desc) = 1 ) pm on upper(pm.master_sku) = upper(a.sku) ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            