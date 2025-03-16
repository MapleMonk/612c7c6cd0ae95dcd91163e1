{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table hox_db.maplemonk.HOX_YFL_7chef_tax_summary as select distinct o.*, case when sales_type in (\'sales\', \'only cogs no sale\') then QUANTITY when sales_type in (\'no sales\', \'stn\') then 0 else null end as new_quantity, case when sales_type in (\'sales\') then INVOICE_AMT when sales_type in (\'no sales\', \'stn\', \'only cogs no sale\') then 0 else null end as new_invoice_amt, case when sales_type in (\'sales\') then SELLING_PRICE when sales_type in (\'no sales\', \'stn\', \'only cogs no sale\') then 0 else null end as new_selling_price, case when sales_type in (\'sales\') then ABSOLUTE_INVOICE_AMOUNT when sales_type in (\'no sales\', \'stn\', \'only cogs no sale\') then 0 else null end as new_abs_invoice_amt, case when sales_type in (\'sales\') then TAX_EXCLUSIVE_GROSS when sales_type in (\'no sales\', \'stn\', \'only cogs no sale\') then 0 else null end as new_tax_exclusive_gross, case when sales_type in (\'sales\') then ABS_TAX_EXCLUSIVE_GROSS when sales_type in (\'no sales\', \'stn\', \'only cogs no sale\') then 0 else null end as new_abs_tax_exclusive_gross from ( select distinct a.*, b.packaging_box, div0(b.packaging_cost, count(1) over (partition by a.Reference_Code, lower(a.REPORT_TYPE), lower(a.MARKETPLACE), lower(a.sku) order by 1)) as PACKAGING_COST, case when lower(a.marketplace) like \'%_stn\' then \'stn\' when lower(a.marketplace) like any (\'%promotional%\', \'redispatch\', \'retail customers\') then \'no sale\' when lower(a.marketplace) like any (\'redispatch_cogs\' , \'shopify sp cogs\') then \'only cogs no sale\' else \'sales\' end as sales_type from hox_db.maplemonk.HOX_yfl_sevenchef_Tax_consolidated_report_intermediate a left join hox_db.maplemonk.HOX_YFL_packaging_material b on a.REFERENCE_CODE = b.REFERENCE_CODE and lower(a.marketplace) = lower(b.marketplace) and lower(a.report_type) = lower(b.report_type) and lower(a.sku) = lower(b.sku) )as o",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from HOX_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            