{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.pronk_inventory_grn_cost_fact_items as select i.sku, data_fetch_date, maplemonk.get_category(ifnull(i.sku,\'\')) as Category, maplemonk.get_sub_category(ifnull(i.sku,\'\')) as Product_Sub_Category, maplemonk.get_color(ifnull(i.sku,\'\')) as Colour, maplemonk.get_size(ifnull(i.sku,\'\')) as Size, maplemonk.get_dt_code(ifnull(i.sku,\'\')) as Product_DT_Code, maplemonk.get_gender(ifnull(i.sku,\'\')) as Gender, available_inventory, fi.avg_grn_cost as grn_cost, case when (Product_DT_Code is null or Product_DT_Code = \'NA\') then fi.avg_grn_cost else (fi.avg_grn_cost + 60) end as avg_grn_cost from maplemonk.pronk_inventory_fact_items i left join (select upper(maplemonk.get_sku_without_dt_code(ifnull(trim(sku),\'\'))) as sku, avg(cast(grn_cost as float64)) as avg_grn_cost from maplemonk.Easyecom_full_inventory_report group by 1 qualify row_number() over (partition by sku order by 1)=1 )fi on lower(fi.sku) = lower(maplemonk.get_sku_without_dt_code(i.sku)) where data_Fetch_date = (select max(data_fetch_date) from maplemonk.pronk_inventory_fact_items);",
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
            