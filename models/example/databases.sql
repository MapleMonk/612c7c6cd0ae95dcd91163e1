{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.offline_store_str as with starting_inv as ( select upper(trim(sku_group)) as sku_group, date, branch_code, sum(inventory) as inventory from snitch_db.maplemonk.offline_master_daily_report_1 where date >= \'2024-11-14\' group by 1,2,3 ), sales_data_dod as ( select upper(trim(sku_group)) as sku_group, order_date::date as order_date, branch_code, sum(shipping_quantity) as sold_quant from snitch_db.maplemonk.Store_Fact_items_offline where marketplace_mapped not like \'%WH%\' and lower(sku_group) not like \'cb%\' and order_date >= \'2024-11-14\' group by 1,2,3 ), sales_inv as ( select a.*, sum(b.sold_quant) as sold_quant30 from starting_inv a left join sales_data_dod b on a.sku_group = b.sku_group and a.branch_code = b.branch_code and a.date < b.order_date and LEAST(DATEADD(DAY, 30, a.DATE), CURRENT_DATE) >= b.order_date group by 1,2,3,4 ), branch_code_marketplace_mapped as ( select distinct branch_code,marketplace_mapped from snitch_db.maplemonk.Store_Fact_items_offline ), outwards as ( select date, from_party as marketplace, b.branch_code, ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(upper(trim(a.sku)), \'-\'), 0, 2), \'-\') AS sku_group, sum(item_quantity) as outward_quant from snitch_db.maplemonk.sto_analysis a left join branch_code_marketplace_mapped b on a.from_party = b.marketplace_mapped where from_party not like \'%- WH -%\' group by 1,2,3,4 ), main_date_cat_pre as ( select a.*,case when inventory >= ifnull(sold_quant30,0) then ifnull(sold_quant30,0) else inventory end as sold_quant30_final, sum(ifnull(b.outward_quant,0)) as outward_quant30 from sales_inv a left join outwards b on a.sku_group = b.sku_group and a.branch_code = b.branch_code and a.date < b.date and LEAST(DATEADD(DAY, 30, a.DATE), CURRENT_DATE) >= b.date group by 1,2,3,4,5,6 ), cluster_data as ( select distinct \"STORE CODE\" AS branch_code, cluster from snitch_db.maplemonk.master_file ), recent_marektplace as ( select distinct branch_code, marketplace_mapped from snitch_db.maplemonk.Store_Fact_items_offline ) select a.*,b.category,c.cluster,d.marketplace_mapped from main_date_cat_pre a left join snitch_db.maplemonk.availability_master_v2 b on upper(trim(a.sku_group)) = upper(trim(b.sku_group)) left join cluster_data c on a.branch_code = c.branch_code left join recent_marektplace d on a.branch_code = d.branch_code ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            