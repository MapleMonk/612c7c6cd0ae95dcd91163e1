{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.beat_material_KPI as select t1.date, t1.region as \"Procurement_Region\", t2.beat_number, t2.beat_name, t2.area, t2.beat_status, t2.Short, t2.Short_eggs, t2.Fresh_Short, t2.Fresh_Short_Eggs, t2.Old_Short, t2.Old_Short_Eggs, t2.jse, t2.loading_point, t2.sku_count, t2.sku, t2.short_name, t2.zone_name, t2.name, ifnull(t2.demand,0) demand, ifnull(t2.out,0) out, ifnull(t2.sold,0) sold, ifnull(t2.promo,0) promo, ifnull(t2.damage,0) damage, ifnull(t2.old_damage,0) old_damage, ifnull(t2.logistics_fresh_missing,0) logistics_fresh_missing, ifnull(t2.logistics_old_missing,0) logistics_old_missing, t2.in_remarks, t2.in_old_remarks, ifnull(t2.return,0) return, ifnull(t2.supply,0) supply, ifnull(t2.fresh_in,0) fresh_in, ifnull(t2.transfer,0) transfer, ifnull(t2.replacement,0) replacement, ifnull(t2.old_in,0) old_in, ifnull(t2.logistics_fresh_missing_eggs,0) logistics_fresh_missing_eggs, ifnull(t2.logistics_old_missing_eggs,0) logistics_old_missing_eggs, ifnull(t2.demand_eggs,0) demand_eggs, ifnull(t2.out_eggs,0) out_eggs, ifnull(t2.sold_eggs,0) sold_eggs, ifnull(t2.replacement_eggs,0) replacement_eggs, ifnull(t2.return_eggs,0) return_eggs, ifnull(t2.fresh_in_eggs,0) fresh_in_eggs, ifnull(t2.damage_eggs,0) damage_eggs, ifnull(t2.old_damage_eggs,0) old_damage_eggs, ifnull(t2.promo_eggs,0) promo_eggs, ifnull(t2.supply_eggs,0) supply_eggs, ifnull(t2.transfer_eggs,0) transfer_eggs, ifnull(t2.old_in_eggs,0) old_in_eggs from (select date::date date, region from eggozdb.maplemonk.date_region_dim where region <> \'U.P\') t1 left join ( select b.beat_number beat_number ,b.beat_name beat_name ,cast(timestampadd(minute,330,dateadd(hour, 5.5, b.beat_date)) as date) AS date ,b.demand_classification area ,b.beat_status ,CASE WHEN b.demand_classification IN (\'Gurgaon-GT\',\'Delhi-GT\',\'NCR-OF-MT\',\'Noida-GT\',\'NCR-MT\',\'NCR-ON-MT\',\'NCR-HORECA\',\'Allahabad-GT\',\'Lucknow-GT\', \'UP-MT\',\'UP-ON-MT\',\'UP-OF-MT\',\'Indore-GT\',\'Bhopal-GT\',\'MP-ON-MT\',\'MP-OF-MT\',\'Chandigarh-GT\',\'Mumbai-ON-MT\',\'Jaipur-GT\',\'NCR-GT\') THEN \'NCR\' WHEN b.demand_classification IN(\'Bangalore-Horeca\',\'Bangalore-MT\',\'Bangalore-GT\',\'Bangalore-ON-MT\',\'Bangalore-OF-MT\',\'Chennai-ON-MT\',\'Chennai-OF-MT\') THEN \'Bangalore\' WHEN b.demand_classification LIKE \'Hyderabad%\' THEN \'Hyderabad\' WHEN b.demand_classification IN (\'East-MT\',\'East-ON-MT\', \'East-OF-MT\', \'Patna-GT\',\'Kolkata-GT\',\'D2C\') THEN \'NCR\' ELSE \'Others\' END AS \"Procurement_Region\" ,cau.name jse ,ww.name as loading_point ,pp.sku_count ,concat(pp.sku_count,pp.short_name) sku ,pp.name ,pp.short_name ,bz.zone_name ,in_remarks ,in_old_remarks ,sum(product_quantity) demand ,sum(product_out_quantity) out ,sum(product_sold_quantity) sold ,sum(product_promo_quantity) promo ,sum(product_damage_quantity) damage ,sum(product_old_damage_quantity) old_damage ,sum(product_logistics_fresh_missing_quantity) logistics_fresh_missing ,sum(product_logistics_old_missing_quantity) logistics_old_missing ,sum(product_return_quantity) return ,sum(product_supply_quantity) supply ,sum(product_fresh_in_quantity) fresh_in ,sum(product_transfer_quantity) transfer ,sum(product_replacement_quantity) replacement ,sum(product_return_replace_in_quantity) old_in ,sum(product_logistics_fresh_missing_quantity)*pp.sku_count logistics_fresh_missing_eggs ,sum(product_logistics_old_missing_quantity)*pp.sku_count logistics_old_missing_eggs ,sum(product_quantity)*pp.sku_count demand_eggs ,sum(product_out_quantity)*pp.sku_count out_eggs ,sum(product_sold_quantity)*pp.sku_count sold_eggs ,sum(product_replacement_quantity)*pp.sku_count replacement_eggs ,sum(product_return_quantity)*pp.sku_count return_eggs ,sum(product_fresh_in_quantity)*pp.sku_count fresh_in_eggs ,sum(product_damage_quantity)*pp.sku_count damage_eggs ,sum(product_old_damage_quantity)*pp.sku_count old_damage_eggs ,sum(product_promo_quantity)*pp.sku_count promo_eggs ,sum(product_supply_quantity)*pp.sku_count supply_eggs ,sum(product_transfer_quantity)*pp.sku_count transfer_eggs ,sum(product_return_replace_in_quantity)*pp.sku_count old_in_eggs ,(out+transfer-sold+return-promo-fresh_in-old_in-damage-old_damage-logistics_fresh_missing-logistics_old_missing) as Short ,(out_eggs+transfer_eggs-sold_eggs+return_eggs-promo_eggs-fresh_in_eggs-old_in_eggs-damage_eggs-old_damage_eggs-logistics_fresh_missing_eggs-logistics_old_missing_eggs) as Short_eggs ,(out+transfer-sold-replacement-promo-fresh_in-damage) as Fresh_Short ,(out_eggs+transfer_eggs-sold_eggs-replacement_eggs-promo_eggs-fresh_in_eggs-damage_eggs) as Fresh_Short_Eggs ,(return+replacement-old_damage-old_in) as Old_Short ,(return_eggs+replacement_eggs-old_damage_eggs-old_in_eggs) as Old_Short_Eggs from eggozdb.maplemonk.my_sql_saleschain_salesdemandsku s left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment b on s.beatassignment_id = b.id left join eggozdb.maplemonk.my_sql_warehouse_warehouse ww on ww.id = b.warehouse_id left join eggozdb.maplemonk.my_sql_base_zone bz on bz.id = ww.zone_id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = s.product_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile d on d.id = b.jse_id LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = d.user_id where lower(b.supply_approval_status)<>\'cancelled\' and lower(pp.slug) not like \'%djn%\' group by b.beat_number ,b.beat_name ,date ,s.in_remarks ,s.in_old_remarks ,b.demand_classification ,b.beat_status ,cau.name ,pp.sku_count ,concat(pp.sku_count,pp.name) ,pp.short_name ,ww.name ,pp.name ,bz.zone_name ) t2 on t1.date = t2.date and t1.region = t2.\"Procurement_Region\" ; create or replace table eggozdb.maplemonk.demand_supply_sold_procure as select coalesce(a.date,b.GRN_DATE) as date, a.demanded, a.supplied, a.sold, a.return, a.fresh_in, a.replaced, ifnull(b.procured, 0) as procured, coalesce(b.region, a.\"Procurement_Region\") as region, case when b.procured is null or b.procured=0 then 0 else ifnull(a.sold,0)/b.procured end as soldvsproc, case when b.procured is null or b.procured=0 then 0 else (ifnull(a.sold,0)-ifnull(a.return,0))/b.procured end as netsoldvsproc from ( select date,\"Procurement_Region\", sum(demand_eggs) as demanded, sum(out_eggs) as supplied, sum(sold_eggs) as sold, sum(return_eggs) as return, sum(fresh_in_eggs) as fresh_in, sum(replacement_eggs) as replaced from maplemonk.beat_material_kpi where date between \'2023-01-01\' and current_date() and name is not null group by date, \"Procurement_Region\" ) a full outer join ( select coalesce(uc.date,rmpd.grn_date) as grn_date, coalesce(uc.zone_name,rmpd.region) as region, ifnull(rmpd.procured,0) as procured, ifnull(uc.opening_eggs,0) op , ifnull(rmpd.procured,0) proc, ifnull(uc.closing_eggs,0) cl from (select date, zone_name, sum(quantity) closing_eggs, lag(sum(quantity),1) over (order by date) opening_eggs from uncleanedclosingstock group by date, zone_name) uc full outer join ( select Region, GRN_DATE, sum(\"EGGS\") as procured from maplemonk.region_wise_procurement_masterdata rmpd where lower(rmpd.type) like any (\'%brown%\',\'%white%\',\'%nutra+%\') group by Region, GRN_DATE ) rmpd on rmpd.grn_date = uc.date and rmpd.region = uc.zone_name ) b on a.date = b.GRN_DATE and a.\"Procurement_Region\" = b.Region where coalesce(a.date,b.GRN_DATE) is not null ; create or replace table eggozdb.maplemonk.moving_average_sold_rep_procure as select a.*, ifnull(b.procured, 0) as procured, ifnull(b.region, a.\"Procurement_Region\") as region, case when b.procured is null or b.procured=0 then 0 else a.sold/b.procured end as soldvsproc, case when b.procured is null or b.procured=0 then 0 else (a.sold-a.return)/b.procured end as netsoldvsproc from (select date,\"Procurement_Region\", sum(sold_eggs) as sold, sum(return_eggs) as return, sum(replacement_eggs) as replaced from maplemonk.beat_material_kpi group by date, \"Procurement_Region\") a join (select Region, GRN_DATE, sum(\"EGGS\") as procured from maplemonk.region_wise_procurement_masterdata rmpd where lower(rmpd.type) in (\'brown\',\'white\',\'nutra+\') group by Region, GRN_DATE) b on a.date = b.GRN_DATE and a.\"Procurement_Region\" = b.Region ; create or replace table eggozdb.maplemonk.cumulative_demand_supply as select tt.date, tt.area, tt.demand, tt.supply, avg(tt.demand) over (partition by tt.area order by tt.date asc rows between unbounded preceding and current row) cumulative_avg_demand, avg(tt.supply) over (partition by tt.area order by tt.date asc rows between unbounded preceding and current row) cumulative_avg_supply from ( select date, area, sum(demand_eggs) as demand, sum(out_eggs) as supply from eggozdb.maplemonk.beat_material_kpi group by date, area ) tt where tt.date >= date_trunc(\'month\', cast(timestampadd(minute, 660, getdate()) as date)) and tt.date <= cast(timestampadd(minute, 660, getdate()) as date) ; create or replace table eggozdb.maplemonk.cumulative_sold_vs_procure as select tt.date, tt.region, case when sum(tt.procured) over (partition by tt.region, tt.date, month(tt.date), year(tt.date) order by tt.date)=0 then 0 else (sum(tt.netsold) over (partition by tt.region, tt.date, month(tt.date), year(tt.date) order by tt.date))/(sum(tt.procured) over (partition by tt.region , tt.date, month(tt.date), year(tt.date) order by tt.date)) end as dailynetsoldvsproc, case when sum(tt.procured) over (partition by tt.region, month(tt.date), year(tt.date) order by tt.date asc rows between unbounded preceding and current row) = 0 then 0 else (sum(tt.netsold) over (partition by tt.region, month(tt.date), year(tt.date) order by tt.date asc rows between unbounded preceding and current row))/(sum(tt.procured) over (partition by tt.region , month(tt.date), year(tt.date) order by tt.date asc rows between unbounded preceding and current row)) end as cumulative_netsoldvsproc from (select date::date date, region, sum(sold)-sum(return) as netsold, sum(procured) as procured from eggozdb.maplemonk.demand_supply_sold_procure group by region, date ) tt ; create or replace table eggozdb.maplemonk.demand_supply_sold_procure_typewise as select c.date, c.region as \"Procurement_Region\", c.sku_type as type, ifnull(a.demanded,0) demanded, ifnull(a.supplied,0) supplied, ifnull(a.sold,0) sold, ifnull(a.return,0) return, ifnull(a.fresh_in,0) fresh_in, ifnull(a.replaced,0) replaced, ifnull(b.procured, 0) as procured, ifnull(b.region, c.region) as region, case when b.procured is null or b.procured=0 then 0 else ifnull(a.sold,0)/b.procured end as soldvsproc, case when b.procured is null or b.procured=0 then 0 else (ifnull(a.sold,0)-ifnull(a.return,0))/b.procured end as netsoldvsproc from (select date, region, sku_type from eggozdb.maplemonk.date_region_skutype where region in (\'NCR\',\'M.P\',\'Bangalore\',\'East\') and sku_type in (\'White\',\'Brown\')) c left join (select date,\"Procurement_Region\", case when lower(sku) like \'%white%\' or lower(sku) like \'%nutra%\' then \'White\' when lower(sku) like \'%brown%\' or lower(sku) like \'%free range%\' then \'Brown\' else \'Others\' end as type, sum(demand_eggs) as demanded, sum(out_eggs) as supplied, sum(sold_eggs) as sold, sum(return_eggs) as return, sum(fresh_in_eggs) as fresh_in, sum(replacement_eggs) as replaced from maplemonk.beat_material_kpi group by case when lower(sku) like \'%white%\' or lower(sku) like \'%nutra%\' then \'White\' when lower(sku) like \'%brown%\' or lower(sku) like \'%free range%\' then \'Brown\' else \'Others\' end, date, \"Procurement_Region\") a on a.date = c.date and a.\"Procurement_Region\" = c.region and a.type = c.sku_type left join (select Region, GRN_DATE, sum(\"EGGS\") as procured, case when lower(rmpd.type) = \'white\' or lower(rmpd.type) = \'nutra+\' then \'White\' when lower(rmpd.type) = \'brown\' then \'Brown\' else \'Others\' end as type from maplemonk.region_wise_procurement_masterdata rmpd where lower(rmpd.type) in (\'brown\',\'white\',\'nutra+\') group by Region, GRN_DATE, case when lower(rmpd.type) = \'white\' or lower(rmpd.type) = \'nutra+\' then \'White\' when lower(rmpd.type) = \'brown\' then \'Brown\' else \'Others\' end ) b on c.date = b.GRN_DATE and c.region = b.Region and c.sku_type = b.type ; create or replace table eggozdb.maplemonk.cumulative_sold_vs_procure_typewise as select tt.date, tt.region, tt.type, case when sum(tt.procured) over (partition by tt.region, tt.type, tt.date, month(tt.date), year(tt.date) order by tt.date)=0 then 0 else (sum(tt.netsold) over (partition by tt.region, tt.type, tt.date, month(tt.date), year(tt.date) order by tt.date))/(sum(tt.procured) over (partition by tt.region, tt.type, tt.date, month(tt.date), year(tt.date) order by tt.date)) end as dailynetsoldvsproc, case when sum(tt.procured) over (partition by tt.region, tt.type, month(tt.date), year(tt.date) order by tt.date asc rows between unbounded preceding and current row) = 0 then 0 else (sum(tt.netsold) over (partition by tt.region, tt.type, month(tt.date), year(tt.date) order by tt.date asc rows between unbounded preceding and current row))/(sum(tt.procured) over (partition by tt.region, tt.type, month(tt.date), year(tt.date) order by tt.date asc rows between unbounded preceding and current row)) end as cumulative_netsoldvsproc from (select date::date date, region, type, sum(sold)-sum(return) as netsold, sum(procured) as procured from eggozdb.maplemonk.demand_supply_sold_procure_typewise group by region, date, type ) tt ; create or replace table eggozdb.maplemonk.demand_supply_sold_procurement_Chart as Select aa.Total_Demand , aa.Total_Supplied, aa.Total_Sold, aa.Total_Replacement, aa.Total_Fresh_In, aa.Total_Return, bb.White_Demand, bb.White_Supply , bb.White_Sold, bb.White_Replacement, bb.White_Fresh_In, bb.White_Return, cc.Brown_Demand, cc.Brown_Supply , cc.Brown_Sold, cc.Brown_Replacement, cc.Brown_Fresh_In, cc.Brown_Return ,dd.* from ( select wf.date, wf.Procurement_region, sum(wf.Demand) as Total_Demand , sum(wf.Supplied) as Total_Supplied, sum(wf.sold) as Total_Sold, sum(wf.replacement)as Total_Replacement, sum(wf.fresh_in) as Total_Fresh_In, sum(wf.return) as Total_Return from ( SELECT DATE_TRUNC(\'DAY\', date) AS date, \"Procurement_Region\" AS Procurement_Region, case when sku in (\'25Brown\',\'30Brown\',\'210Brown\', \'6Brown\' ,\'10Brown\' ) then \'Brown\' when sku in (\'25White\',\'30White\',\'210White\', \'6White\' ,\'10White\' , \'12White\' , \'10Nutra\', \'1Liquid\') then \'White\' when sku in (\'12Free Range\', \'6Free Range\') then \'Free Range\' else \'Others\' end as Type1, sum(demand_Eggs) AS Demand, sum(out_Eggs) AS Supplied, sum(sold_Eggs) AS Sold, sum(replacement_eggs) AS Replacement, sum(fresh_in_eggs) AS Fresh_In, sum(return_eggs) AS Return FROM maplemonk.beat_material_kpi GROUP BY sku, Procurement_Region, DATE_TRUNC(\'DAY\', date) )wf group by wf.DATE , wf.Procurement_Region )aa left join ( select wf.date, wf.Procurement_region, sum(wf.Demand) as White_Demand , sum(wf.Supplied) as White_Supply, sum(wf.sold) as White_Sold, sum(wf.replacement)as White_Replacement, sum(wf.fresh_in) as White_Fresh_In, sum(wf.return) as White_Return from ( SELECT DATE_TRUNC(\'DAY\', date) AS date, \"Procurement_Region\" AS Procurement_Region, case when sku in (\'25Brown\',\'30Brown\',\'210Brown\', \'6Brown\' ,\'10Brown\' ) then \'Brown\' when sku in (\'25White\',\'30White\',\'210White\', \'6White\' ,\'10White\' , \'12White\' , \'10Nutra\', \'1Liquid\') then \'White\' when sku in (\'12Free Range\', \'6Free Range\') then \'Free Range\' else \'Others\' end as Type1, sum(demand_Eggs) AS Demand, sum(out_eggs) AS Supplied, sum(sold_eggs) AS Sold, sum(replacement_eggs) AS Replacement, sum(fresh_in_eggs) AS Fresh_In, sum(return_eggs) AS Return FROM maplemonk.beat_material_kpi GROUP BY sku, Procurement_Region, DATE_TRUNC(\'DAY\', date) )wf where wf.Type1 = \'White\' group by wf.DATE , wf.Procurement_Region, wf.type1 )bb on aa.date = bb.date and aa.Procurement_region = bb.Procurement_Region left join ( select wf.date, wf.Procurement_region, sum(wf.Demand) as Brown_Demand , sum(wf.Supplied) as Brown_Supply, sum(wf.sold) as Brown_Sold, sum(wf.replacement)as Brown_Replacement, sum(wf.fresh_in) as Brown_Fresh_In, sum(wf.return) as Brown_Return from ( SELECT DATE_TRUNC(\'DAY\', date) AS date, \"Procurement_Region\" AS Procurement_Region, case when sku in (\'25Brown\',\'30Brown\',\'210Brown\', \'6Brown\' ,\'10Brown\' ) then \'Brown\' when sku in (\'25Brown\',\'30Brown\',\'210Brown\', \'6Brown\' ,\'10Brown\' , \'12Brown\' , \'10Nutra\', \'1Liquid\') then \'Brown\' when sku in (\'12Free Range\', \'6Free Range\') then \'Free Range\' else \'Others\' end as Type1, sum(demand_Eggs) AS Demand, sum(out_Eggs) AS Supplied, sum(sold_Eggs) AS Sold, sum(replacement_eggs) AS Replacement, sum(fresh_in_eggs) AS Fresh_In, sum(return_eggs) AS Return FROM maplemonk.beat_material_kpi GROUP BY sku, Procurement_Region, DATE_TRUNC(\'DAY\', date) )wf where wf.Type1 = \'Brown\' group by wf.DATE , wf.Procurement_Region, wf.type1 )cc on aa.date = cc.date and aa.Procurement_region = cc.Procurement_Region left join ( Select DISTINCT xx.GRN_DATE, xx.region, xx.Total_Procurred_Egg,xx.Amount, yy.Procurred_Egg_White, yy.Amount_White,zz.Procurred_Egg_Brown,zz.Amount_Brown, gg.Procurred_Egg_Nutra, gg.Amount_Nutra, div0null(xx.Amount,xx.Total_Procurred_Egg) as Proc_Price , div0null(yy.Amount_White,yy.Procurred_Egg_White) as Procured_Price_White , div0null(zz.Amount_Brown,zz.Procurred_Egg_Brown) as Procured_Price_Brown , div0null(gg.Amount_Nutra,gg.Procurred_Egg_Nutra) as Procured_Price_Nutra From ( SELECT GRN_DATE, region, sum(EGGS) as Total_Procurred_Egg, sum(amount) as Amount from eggozdb.maplemonk.region_wise_procurement_masterdata group by 1 ,2 ) xx full outer join (SELECT GRN_DATE, region, sum(EGGS) as Procurred_Egg_White , sum(amount) as Amount_White from eggozdb.maplemonk.region_wise_procurement_masterdata where type =\'White\' group by 1,2 )yy on xx.GRN_DATE= yy.GRN_DATE and xx.region = yy.region full outer join ( SELECT GRN_DATE, region, sum(EGGS) as Procurred_Egg_Brown , sum(amount) as Amount_Brown from eggozdb.maplemonk.region_wise_procurement_masterdata where type = \'Brown\' group by 1,2 ) zz on xx.GRN_DATE = zz.GRN_DATE and xx.region = zz.region full outer join ( SELECT GRN_DATE, region ,sum(EGGS) as Procurred_Egg_Nutra, sum(amount) as Amount_Nutra from eggozdb.maplemonk.region_wise_procurement_masterdata where type = \'Nutra+\' group by 1,2 ) gg on xx.GRN_DATE = gg.GRN_DATE and xx.region= gg.region )dd on aa.date = dd.GRN_DATE and aa.Procurement_region = dd.Region ; create or replace table eggozdb.maplemonk.retailer_demand_supply as select cast(timestampadd(minute,660,dateadd(hour, 5.5, b.beat_date)) as date) AS date ,b.demand_classification area ,product_type_to_eggoz_segment(pp.product_type) as brand_segment ,pp.product_type ,b.beat_number beat_number ,b.beat_name beat_name ,pp.sku_count ,concat(pp.sku_count,pp.short_name) sku ,(s.product_quantity) demand ,(s.product_quantity*pp.sku_count) demand_eggs ,(s.product_out_quantity) out ,(s.product_out_quantity*pp.sku_count) out_eggs ,(s.product_supply_quantity) supply ,(s.product_supply_quantity*pp.sku_count) supply_eggs from eggozdb.maplemonk.my_sql_distributionchain_beatassignment b left join eggozdb.maplemonk.my_sql_saleschain_salesdemandsku s on s.beatassignment_id = b.id left join eggozdb.maplemonk.my_sql_warehouse_warehouse ww on ww.id = b.warehouse_id left join eggozdb.maplemonk.my_sql_base_zone bz on bz.id = ww.zone_id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = s.product_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile d on d.id = b.jse_id LEFT JOIN eggozdb.Maplemonk.my_sql_custom_auth_user cau ON cau.id = d.user_id where lower(b.supply_approval_status)<>\'cancelled\' and lower(pp.slug) not like \'%djn%\' group by b.beat_number,s.product_quantity , b.beat_date ,b.beat_name ,S.PRODUCT_OUT_QUANTITY ,S.PRODUCT_SUPPLY_QUANTITY ,b.demand_classification ,pp.sku_count ,pp.product_type ,concat(pp.sku_count,pp.name) ,pp.short_name ;",
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
                        