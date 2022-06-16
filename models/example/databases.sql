{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.beat_material_KPI as select b.beat_number beat_number ,b.beat_name beat_name ,cast(timestampadd(minute,330,dateadd(hour, 5.5, b.beat_date)) as date) AS date ,b.demand_classification area ,cau.name FSE ,pp.sku_count ,concat(pp.sku_count,pp.name) sku ,sum(product_quantity) demand ,sum(product_out_quantity) out ,sum(product_sold_quantity) sold ,sum(product_promo_quantity) promo ,sum(product_damage_quantity) damage ,sum(product_return_quantity) return ,sum(product_supply_quantity) supply ,sum(product_fresh_in_quantity) fresh_in ,sum(product_transfer_quantity) transfer ,sum(product_replacement_quantity) replacement ,sum(product_return_replace_in_quantity) old_in ,case when sum(product_quantity)=0 then 0 ELSE -1*(sum(product_quantity) - sum(product_out_quantity))/sum(product_quantity) END as demand_vs_out ,SUM(product_out_quantity)-SUM(product_fresh_in_quantity) - SUM(product_damage_quantity) as net_out_by_warehouse ,SUM(product_sold_quantity)+SUM(product_replacement_quantity)+SUM(product_promo_quantity)-SUM(product_transfer_quantity) as net_out_by_sales ,case when (SUM(product_out_quantity)-SUM(product_fresh_in_quantity) - SUM(product_damage_quantity)) = 0 then 0 else (SUM(product_out_quantity)-SUM(product_fresh_in_quantity) - SUM(product_damage_quantity)-(SUM(product_sold_quantity)+SUM(product_replacement_quantity)+SUM(product_promo_quantity)-SUM(product_transfer_quantity)))/(SUM(product_out_quantity)-SUM(product_fresh_in_quantity) - SUM(product_damage_quantity)) end as deviation_on_beat_closing ,sum(product_return_replace_in_quantity) - sum(product_replacement_quantity)- sum(product_return_quantity) as old_eggs_deviation from eggozdb.maplemonk.my_sql_saleschain_salesdemandsku s left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment b on s.beatassignment_id = b.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = s.product_id left join eggozdb.maplemonk.my_sql_distributionchain_distributionpersonprofile d on d.id = b.distributor_id LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = d.user_id where b.supply_approval_status<>\'Cancelled\' group by b.beat_number ,b.beat_name ,date ,b.demand_classification ,cau.name ,pp.sku_count ,concat(pp.sku_count,pp.name)",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        