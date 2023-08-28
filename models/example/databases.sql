{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.truegradient_suggested_sku_including_pkd_date as select retailer_name, sku, sku_count, retailer_id, area_classification, beat_number, entry_date, total_soh_qty, not_expired_qty, expired_qty, total_eggs_placed_suggested_packs, eggs_placed_suggested_packs_10days, net_placement_required, expired_qty as replacement_packs, case when entry_date is null then eggs_placed_suggested_packs_10days when not_expired_qty + expired_qty >= eggs_placed_suggested_packs_10days then 0 else eggs_placed_suggested_packs_10days - (not_expired_qty + expired_qty) end as fresh_placement_packs from ( select b.retailer_name, b.sku, b.area_classification, b.beat_number_original as beat_number, a.sku_count, a.retailer_id, a.entry_date, a.total_soh_qty, a.not_expired_qty, a.expired_qty, (b.total_eggs_placed_suggested/pp.sku_count) as total_eggs_placed_suggested_packs, cast(((b.total_eggs_placed_suggested/pp.sku_count)*10/30) as int) as eggs_placed_suggested_packs_10days, (cast(((b.total_eggs_placed_suggested/pp.sku_count)*10/30) as int) - not_expired_qty) as net_placement_required from ( select *, (\"Eggs Sold_new\" + \"Eggs Replaced_new\") as total_eggs_placed_suggested from eggozdb.maplemonk.TRUEGRADIENT_REPLACEMENT_OPTIMIZATION_2 where experiment_id=\'d7ab23b1-9f03-4187-9c6b-9c6cbb8fca9f\' ) b left join (select distinct sku_count,short_name from eggozdb.maplemonk.my_sql_product_product) pp on b.sku = concat(pp.sku_count,pp.short_name) left join ( select retailer_id,retailer_name,area_classification,beat_number,sku,sku_count,entry_date, sum(ifnull(quantity,0)) as total_soh_qty, sum(case when egg_status=\'not expired\' then quantity else 0 end) as not_expired_qty, sum(case when egg_status=\'expired\' then quantity else 0 end) as expired_qty from ( select t1.retailer_id, t1.retailer_name, t1.area_classification, t1.beat_number_original as beat_number, t1.sku, t1.sku_count, t1.entry_date, t2.quantity, t1.model_id, cast(t2.pkd_date as date) as pkd_date_new, (t1.entry_date - cast(t2.pkd_date as date)) as sku_days, case when (t1.entry_date - cast(t2.pkd_date as date)) > 15 then \'expired\' when (t1.entry_date - cast(t2.pkd_date as date)) between 0 and 15 then \'not expired\' else \'NA\' end as egg_status, 15-(t1.entry_date - cast(t2.pkd_date as date)) as days_to_expire from (select * from ( select ROW_NUMBER() over (partition by retailer_name, sku order by entry_date desc) as latestdate_rank, * from eggozdb.maplemonk.eggoz_soh ) where latestdate_rank=1) t1 left join ( select concat(pp.sku_count,pp.short_name) sku, pp.sku_count, oei.pkd_date, cast(timestampadd(minute, 330, oei.modified_at) as date) entry_date, oei.quantity, oe.soh_model_id as model_id from eggozdb.maplemonk.my_sql_order_eggozsoh oe left join eggozdb.maplemonk.my_sql_order_eggozsohinline oei on oei.eggoz_soh_id = oe.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = oe.product_id where cast(timestampadd(minute, 330, oei.modified_at) as date) >= date_trunc(\'month\',dateadd(\'month\',-2,current_date())) ) t2 on t1.model_id = t2.model_id and t1.sku = t2.sku where t1.area_classification in (\'NCR-OF-MT\',\'Gurgaon-GT\',\'Noida-GT\',\'Delhi-GT\') )aa group by retailer_id,retailer_name,area_classification,beat_number,sku,sku_count,entry_date )a on a.retailer_name = b.retailer_name and a.sku = b.sku ) bb",
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
                        