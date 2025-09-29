{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.Neshanka_SKU_Master as select sm.EAN, sm.RPC as commonsku, case when sm.`Set` <> \'\' or sm.`Set` is not null then cast(sm.`Set` as int64) end Set_Of, safe_divide(safe_cast(replace(sm.GST__,\'%\',\'\') as int64),100) GST_Rate, sm.Subcat as sub_Category, sm.Category as Category, replace(sm.GST_Class,\'#N/A\',\'\') as GST_Class, coalesce(sm.Product_name,pm.Product_name) Product_name, replace(sm.New_HSN__17sept_,\'#N/A\',\'\') as New_HSN_from_17_Sept_25, pm.Created_at Product_Launch_date, pm.product_image_url, pm.Brand, pm.cogs, pm.tax_rule_name, pm.product_type, pm.mrp from `maplemonk.GS_Ritualistic_SKU_MASTER` sm left join ( select sku, mrp, cast(cost as float64) as cogs, upper(brand) Brand, product_name, product_type, tax_rule_name, product_image_url, Created_at from maplemonk.easyecom_Easyecom_product_master qualify row_number() over (partition by sku order by updated_at desc) = 1 ) pm on lower(sm.RPC) = lower(pm.sku);",
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
            