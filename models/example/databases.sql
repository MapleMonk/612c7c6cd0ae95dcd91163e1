{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.beat_material_KPI as select b.beat_number beat_number ,b.beat_name beat_name ,cast(timestampadd(minute,330,dateadd(hour, 5.5, b.beat_date)) as date) AS date ,b.demand_classification area ,CASE WHEN b.demand_classification IN (\'Gurgaon-GT\',\'Delhi-GT\',\'NCR-OF-MT\',\'Noida-GT\',\'NCR-MT\',\'NCR-ON-MT\',\'NCR-HORECA\') THEN \'Gurgaon\' WHEN b.demand_classification IN(\'Allahabad-GT\',\'Lucknow-GT\',\'UP-MT\',\'Indore-GT\',\'Bhopal-GT\') THEN \'UP\' WHEN b.demand_classification IN(\'Bangalore-Horeca\',\'Bangalore-MT\',\'Bangalore-GT\') THEN \'Bangalore\' END AS \"Procurement Region\" ,cau.name FSE ,ww.name as loading_point ,pp.sku_count ,concat(pp.sku_count,pp.name) sku ,sum(product_quantity) demand ,sum(product_out_quantity) out ,sum(product_sold_quantity) sold ,sum(product_promo_quantity) promo ,sum(product_damage_quantity) damage ,sum(product_return_quantity) return ,sum(product_supply_quantity) supply ,sum(product_fresh_in_quantity) fresh_in ,sum(product_transfer_quantity) transfer ,sum(product_replacement_quantity) replacement ,sum(product_return_replace_in_quantity) old_in ,sum(product_quantity)*pp.sku_count demand_eggs ,sum(product_out_quantity)*pp.sku_count out_eggs ,sum(product_sold_quantity)*pp.sku_count sold_eggs ,sum(product_replacement_quantity)*pp.sku_count replacement_eggs from eggozdb.maplemonk.my_sql_saleschain_salesdemandsku s left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment b on s.beatassignment_id = b.id left join eggozdb.maplemonk.my_sql_warehouse_warehouse ww on ww.id = b.warehouse_id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = s.product_id left join eggozdb.maplemonk.my_sql_distributionchain_distributionpersonprofile d on d.id = b.distributor_id LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = d.user_id where b.supply_approval_status<>\'Cancelled\' group by b.beat_number ,b.beat_name ,date ,b.demand_classification ,cau.name ,pp.sku_count ,concat(pp.sku_count,pp.name) ,ww.name ; create or replace table eggozdb.maplemonk.demand_supply_sold_procure as select a.*, b.procured from (select date, area, loading_point, sum(demand_eggs) as demanded, sum(out_eggs) as supplied, sum(sold_eggs) as sold, sum(return) as return, sum(fresh_in) as fresh_in, sum(replacement_eggs) as replaced from maplemonk.beat_material_kpi group by date, area, loading_point) a join (select GRN_DATE, sum(\"EGGS\") as procured from maplemonk.region_wise_procurement_masterdata rmpd group by Region, GRN_DATE) b on a.date = b.GRN_DATE ;",
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
                        