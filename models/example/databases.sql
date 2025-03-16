{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table hox_db.maplemonk.HOX_YFL_7chef_tax_summary as select distinct a.*, b.packaging_box, div0(b.packaging_cost, count(1) over (partition by a.Reference_Code, lower(a.REPORT_TYPE), lower(a.MARKETPLACE), lower(a.sku) order by 1)) as PACKAGING_COST from hox_db.maplemonk.HOX_yfl_sevenchef_Tax_consolidated_report_intermediate a left join hox_db.maplemonk.HOX_YFL_packaging_material b on a.REFERENCE_CODE = b.REFERENCE_CODE and lower(a.marketplace) = lower(b.marketplace) and lower(a.report_type) = lower(b.report_type) and lower(a.sku) = lower(b.sku)",
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
            