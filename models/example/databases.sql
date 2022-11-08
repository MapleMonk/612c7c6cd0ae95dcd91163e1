{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.Maplemonk.eggs_sold_new_area as with cte as (select date, case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end area, sku, \'White\' as Type, sum(ifnull(eggs_sold_white,0))- sum(ifnull(eggs_return_white,0)) as net_eggs_sold, sum(ifnull(eggs_replaced_white,0)) + sum(ifnull(eggs_return_white,0)) as return_replaced_Eggs_sold, sum(ifnull(eggs_promo_white,0)) as promo_eggs_sold from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by date, case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end, sku UNION select date, case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end area, sku, \'Brown\' as Type, sum(ifnull(eggs_sold_brown,0))- sum(ifnull(eggs_return_brown,0)) as net_eggs_sold, sum(ifnull(eggs_replaced_brown,0)) + sum(ifnull(eggs_return_brown,0)) as return_replaced_Eggs_sold, sum(ifnull(eggs_promo_brown,0)) as promo_eggs_sold from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by date, case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end, sku UNION select date, case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end area, sku, \'Nutra Plus\' as Type, sum(ifnull(eggs_sold_nutra,0)) - sum(ifnull(eggs_return_nutra,0)) as net_eggs_sold, sum(ifnull(eggs_replaced_nutra,0)) + sum(ifnull(eggs_return_nutra,0)) as return_replaced_Eggs_sold, sum(ifnull(eggs_promo_nutra,0)) as promo_eggs_sold from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by date, case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end, sku ) select date, area, sku, type, net_eggs_sold, return_replaced_Eggs_sold, promo_eggs_sold from cte; create or replace table eggozdb.Maplemonk.procurement_new_area as select p.date, e.area, sum(case when lower(p.type_new) = \'white\' then e.net_eggs_sold * (p.cost_per_egg + 0.11) when lower(p.type_new) = \'brown\' then e.net_eggs_sold * (p.cost_per_egg + 0.11) when lower(p.type_new) = \'nutra plus\' then e.net_eggs_sold * (p.cost_per_egg + 0.45) end ) as Procurement_total_expense from (select date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)) as date, case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end as type_new, sum(replace(amount,\',\',\'\')::float) as amount, sum(replace(eggs,\',\',\'\')::float) as eggs, div0(sum(replace(amount,\',\',\'\')::float),sum(replace(eggs,\',\',\'\')::float)) as cost_per_egg from eggozdb.Maplemonk.procurement_cost_procure where eggs not in (\'bill total is 78524\',\'GRN missed\',\'2 tray extra\',\'2 tray short\') group by date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)), case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end) p left join (select distinct date, case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end area, type, sum(net_eggs_sold) net_eggs_sold from eggozdb.Maplemonk.eggs_sold_new group by date, area, type) e on p.date = e.date and p.type_new = e.type group by p.date, e.area order by p.date desc; create or replace table eggozdb.Maplemonk.first_mile_new_area as select a.date_new, s.area, div0(a.fm_total_expense,count(distinct area) over (partition by date)) fm_total_expense, case when s.area in (\'NCR-MT\', \'Delhi-GT\', \'Noida-GT\', \'Gurgaon-GT\', \'NCR-HORECA\') then a.total_eggs_procured/5 else 0 end total_eggs_procured_ncr, div0(a.total_eggs_procured, count(distinct area) over (partition by date)) total_eggs_procured from(select fm.date_new, sum(fm.total_expense) fm_total_expense, sum(p.total_eggs) as total_eggs_procured from (select to_date(date,\'DD/MM/YYYY\') as date_new, sum(total_expense) as total_expense from eggozdb.Maplemonk.transport_costs_fm_vehicle_details_after_1st_april group by to_date(date,\'DD/MM/YYYY\')) fm left join (select date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)) as date, sum(replace(eggs,\',\',\'\')::float) as total_eggs from eggozdb.Maplemonk.procurement_cost_procure where eggs not in (\'bill total is 78524\',\'GRN missed\',\'2 tray extra\',\'2 tray short\') group by date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)))p on fm.date_new = p.date group by fm.date_new) a left join (select date, case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end area, sum(eggs_sold) as eggs_sold, sum(eggs_replaced) as eggs_replaced, sum(eggs_return) as eggs_return from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by date,case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end) s on a.date_new=s.date where s.area in (\'NCR-MT\', \'Delhi-GT\', \'Noida-GT\', \'Gurgaon-GT\', \'NCR-HORECA\') ; create or replace table eggozdb.Maplemonk.mid_mile_new_area as select a.date_new, b.area, div0(a.mm_total_expense,b.net_eggs) as mm_total_expense from(select to_date(date,\'dd/mm/yyyy\') as date_new, case when area = \'Delhi\' then \'Delhi-GT\' when area = \'Gurgaon\' then \'Gurgaon-GT\' when area = \'Noida\' then \'Noida-GT\' end as area, sum(case when cost is null then 0 else replace(cost,\',\',\'\')::float end) as mm_total_expense, sum(case when total_eggs_out is null then 0 else total_eggs_out::float end) as total_eggs_out from eggozdb.Maplemonk.transport_costs_last___mid_mile where \"GT/MT\" in (\'MM\',\'WMT\') and cost not in (\'#REF!\',\'#N/A\') group by to_date(date,\'dd/mm/yyyy\'), area )a left join (select date, case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end area, sum(eggs_sold - eggs_return) net_eggs from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_dso where area in (\'Delhi-GT\',\'Noida-GT\',\'NCR-MT\',\'NCR-HORECA\',\'Gurgaon-GT\') and lower(operator) <> \'rajeev\' group by case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end, date )b on a.date_new=b.date and a.area = b.area ; create or replace table eggozdb.Maplemonk.last_mile_new_area as select a.area, a.date, sum(a.cost) cost, sum(b.eggs_sold) eggs_sold, sum(b.eggs_return) eggs_return, sum(a.cost) lm_total_expense from ( select case when \"GT/MT\" in (\'LMT\',\'OFF-MT\',\'ON-MT\') then \'NCR-MT\' when \"GT/MT\"=\'GGT\' then \'Gurgaon-GT\' when \"GT/MT\"=\'DGT\' then \'Delhi-GT\' when \"GT/MT\"=\'NGT\' then \'Noida-GT\' when \"GT/MT\" = \'HORECA\' then \'NCR-HORECA\' end as area, case when beat like \'%hoc%\' or beat like \'%DHOC%\' or beat like \'%Horeca%\' or beat like \'%Sample%\' then 9999 else beat end as beat, to_date(date,\'dd/mm/yyyy\') as date, sum(case when cost is null then 0 else replace(cost,\',\',\'\')::float end) + sum(case when unloading_cost is null then 0 else replace(unloading_cost,\',\',\'\')::float end) as cost from eggozdb.Maplemonk.transport_costs_last___mid_mile where \"GT/MT\" in (\'LMT\',\'GGT\',\'DGT\',\'NGT\',\'OFF-MT\',\'ON-MT\',\'HORECA\') and cost not in (\'#REF!\',\'#N/A\') and beat not in (\'Refilling\',\'RTV\',\'Unbranded\') group by case when beat like \'%hoc%\' or beat like \'%DHOC%\' or beat like \'%Horeca%\' or beat like \'%Sample%\' then 9999 else beat end, date, case when \"GT/MT\" in (\'LMT\',\'OFF-MT\',\'ON-MT\') then \'NCR-MT\' when \"GT/MT\"=\'GGT\' then \'Gurgaon-GT\' when \"GT/MT\"=\'DGT\' then \'Delhi-GT\' when \"GT/MT\"=\'NGT\' then \'Noida-GT\' when \"GT/MT\" = \'HORECA\' then \'NCR-HORECA\' end )a left join ( select date,case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end area, case when beat_number_operations=0 then 9999 else beat_number_operations end as beat, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by date,case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end,beat_number_operations )b on a.beat=b.beat and a.date=b.date and a.area=b.area group by a.area, a.date ; create or replace table eggozdb.Maplemonk.packaging_new_area as select month, date, region, sum(weighted) as packaging_total_expense, sum(weighted_replaced) as packaging_total_expense_replaced, sum(weighted_return) as packaging_total_expense_return, sum(weighted_promo) as packaging_total_expense_promo from ( select a.month, b.date, a.region, a.sku, a.cost, b.eggs_sold, b.eggs_replaced, b.eggs_return, a.cost*(b.eggs_sold-eggs_return) as weighted, a.cost*b.eggs_replaced as weighted_replaced, a.cost*b.eggs_return as weighted_return, a.cost*b.eggs_promo as weighted_promo from ( select region, sku, last_day(to_date(month,\'yyyy/mm/dd\')) as month, sum(packaging_cost) as cost from eggozdb.Maplemonk.packaging_cost_per_sku_per_egg group by region, sku, last_day(to_date(month,\'yyyy/mm/dd\')) )a left join ( select last_day(date) as month, date, sku, case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end region, sum(eggs_sold) as eggs_sold, sum(eggs_replaced) as eggs_replaced, sum(eggs_return) as eggs_return, sum(eggs_promo) as eggs_promo from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by last_day(date),sku ,date ,case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end )b on a.sku=b.sku and a.month=b.month and a.region=b.region ) group by month,region,date ; create or replace table eggozdb.maplemonk.credit_loss_area as select a.date, a.area, a.credit_loss as credit_loss_total from ( select p.transaction_date::date as date, case when rr.area_classification in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else rr.area_classification end area, sum(p.transaction_amount) as credit_loss from eggozdb.maplemonk.my_sql_payment_salestransaction p left join eggozdb.Maplemonk.my_sql_retailer_retailer rr ON p.retailer_id =rr.id where transaction_type in (\'Credit Note\',\'Debit Note\') group by p.transaction_date::date, case when rr.area_classification in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else rr.area_classification end )a left join ( select case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end area, date, sum(net_sales) as net_sales, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return, sum(eggs_replaced) as eggs_replaced from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end,date )b on a.date=b.date and a.area=b.area; create or replace table eggozdb.Maplemonk.sales_salary_new_area as select last_day(to_date(concat(ss.year,\'-\',ss.month,\'-\',\'01\'),\'yyyy-mmmm-dd\')) as month, ss.classification as area, sum(replace(ss.\"Sales Salary\",\',\',\'\')) sales_salary_total from eggozdb.Maplemonk.BP_Sales_Salary_by_area ss left join (select case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end area, last_day(date) as month, sum(net_sales) as net_sales, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return, sum(eggs_replaced) as eggs_replaced from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end, last_day(date))a on a.area = ss.classification and a.month = last_day(to_date(concat(ss.year,\'-\',ss.month,\'-\',\'01\'),\'yyyy-mmmm-dd\')) where ss.classification not in (\'NCR-UB\') group by ss.year,ss.month,ss.classification; create or replace table eggozdb.Maplemonk.ops_salary_new_area as select last_day(to_date(concat(os.year,\'-\',os.month,\'-\',01),\'yyyy-mmmm-dd\')) as month, os.classification as area, sum(ifnull(replace(os.\"Final Ops Salary\",\',\',\'\')::float,0)) as ops_salary_total from eggozdb.Maplemonk.BP_Ops_Salary os left join (select case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end area, last_day(date) as month, sum(net_sales) as net_sales, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return, sum(eggs_replaced) as eggs_replaced from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku where beat_number_operations is not null group by case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end, last_day(date))a on a.month = last_day(to_date(concat(os.year,\'-\',os.month,\'-\',01),\'yyyy-mmmm-dd\')) and a.area=os.classification group by os.year,os.month,os.classification; create or replace table eggozdb.maplemonk.ub_log_area_profitability as select tt1.*, tt2.avg_cp, tt3.avg_cp_mtd, tt4.avg_cp_mtd_for_region from ( SELECT oo.name ,cast(timestampadd(minute,660,oo.delivery_date ) as date) as deliveryDate ,ot.egg_type , case when lower(pp.name) like \'%white%\' then \'White\' when lower(pp.name) like \'%brown%\' then \'Brown\' when lower(pp.name) like \'%nutra%\' then \'Nutra+\' when lower(pp.name) like \'%liquid%\' then \'White\' end as Category , case when rr.area_classification = \'Bangalore-UB\' then \'Banglore\' when rr.area_classification = \'NCR-UB\' then \'NCR\' when rr.area_classification = \'MP-UB\' then \'M.P\' when rr.area_classification = \'UP-UB\' then \'U.P\' when rr.area_classification = \'East-UB\' then \'Bihar\' end as Regions , rr.area_classification , oo.id as order_id , concat(pp.sku_count,pp.name) as SKU , ot.quantity , case when lower(pp.name) like \'liquid\' then (ot.quantity*1000)/35 when SKU = \'1White\' then ot.quantity * pp.SKU_Count * 30 else ot.quantity * pp.SKU_Count end as eggs_sold , case when SKU = \'1White\' then ot.single_sku_rate * 30 else ot.single_sku_rate end as single_sku_rate , ot.single_sku_mrp , ot.single_sku_rate*ot.quantity as amount , pp.SKU_Count , oo.order_brand_type , oo.secondary_status FROM eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_order_orderline ot on ot.order_id = oo.id left join eggozdb.maplemonk.my_sql_product_product pp on ot.product_id = pp.id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id where secondary_status <> \'cancel_approved\' and rr.area_classification like \'%UB%\' and rr.area_classification <> \'UP-UB\' and lower(status) in (\'delivered\',\'completed\') group by oo.name, ot.egg_type ,deliveryDate , rr.area_classification , ot.quantity , ot.single_sku_rate , ot.single_sku_mrp , oo.id , ot.single_sku_rate*ot.quantity , oo.delivery_date , oo.generation_date , pp.SKU_Count , oo.order_brand_type , oo.secondary_status , pp.name ) tt1 left join ( select mm.* from (select grn_date, region, type, row_number() over (partition by grn_date, region, type order by grn_date) as rownumber, (sum(replace(amount,\',\',\'\')::FLOAT) over (partition by grn_date, region, type ))/(sum(replace(eggs,\',\',\'\')::FLOAT) over (partition by grn_date, region, type )) as avg_cp from eggozdb.maplemonk.region_wise_procurement_masterdata where grn_date between date_trunc(\'month\', cast(timestampadd(minute, \'660\', getdate()) as date)) and cast(timestampadd(minute, 660, getdate()) as date)-1) mm where mm.rownumber = 1 ) tt2 on tt1.deliverydate = tt2.grn_date and tt1.regions = tt2.region and tt1.category = tt2.type left join ( select ll.region, ll.type, ll.avg_cp_mtd from (select grn_date, region, type, row_number() over (partition by region, type order by region) as rownumber, (sum(replace(amount,\',\',\'\')::FLOAT) over (partition by region, type ))/(sum(replace(eggs,\',\',\'\')::FLOAT) over (partition by region, type )) as avg_cp_mtd from eggozdb.maplemonk.region_wise_procurement_masterdata where grn_date between date_trunc(\'month\', cast(timestampadd(minute, \'660\', getdate()) as date)) and cast(timestampadd(minute, 660, getdate()) as date)-1) ll where ll.rownumber = 1 ) tt3 on tt1.regions = tt3.region and tt1.category = tt3.type left join ( select nn.region, nn.avg_cp_mtd_for_region from (select grn_date, region, type, row_number() over (partition by region order by region) as rownumber, (sum(replace(amount,\',\',\'\')::FLOAT) over (partition by region))/(sum(replace(eggs,\',\',\'\')::FLOAT) over (partition by region)) as avg_cp_mtd_for_region from eggozdb.maplemonk.region_wise_procurement_masterdata where grn_date between date_trunc(\'month\', cast(timestampadd(minute, \'660\', getdate()) as date)) and cast(timestampadd(minute, 660, getdate()) as date)-1) nn where nn.rownumber = 1 ) tt4 on tt1.regions = tt4.region ; create or replace table eggozdb.Maplemonk.Unbranded_loss_area as select date, region area, sum(ub_loss_total_expense) ub_loss_total_expense, sum(ub_eggs_sold) ub_eggs_sold from( select m.date, \'NCR-MT\' region, sum((m.cost_per_egg - \"Selling Price/Egg\")*UB_eggs_sold) as UB_Loss_total_expense, sum(m.ub_eggs_sold) as ub_eggs_sold from ( select p.date, ub.region, ub.type_new, ub.\"Selling Price/Egg\", case when type_new = \'White\' then p.cost_per_egg + 0.11 when type_new = \'Brown\' then p.cost_per_egg + 0.11 when type_new = \'Nutra Plus\' then p.cost_per_egg + 0.45 end as cost_per_egg, ub.ub_eggs_sold from (select distinct deliverydate date, case when category=\'Nutra\' then \'Nutra Plus\' when category = \'Melted\' then \'White\' else category end as type_new, area_classification region, div0(amount,eggs_sold) \"Selling Price/Egg\", eggs_sold ub_eggs_sold from eggozdb.maplemonk.ub_log_area_profitability where area_classification = \'NCR-UB\' ) ub left join( select date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)) as date, case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end as type, div0(sum(replace(amount,\',\',\'\')::float),sum(replace(eggs,\',\',\'\')::float)) as cost_per_egg from eggozdb.Maplemonk.procurement_cost_procure where eggs not in (\'bill total is 78524\',\'GRN missed\',\'2 tray extra\',\'2 tray short\') group by date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)), case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end) p on p.date = ub.date and p.type = ub.type_new)m group by m.date, m.region order by m.date desc ) group by date, region ; create or replace table eggozdb.maplemonk.ub_FM_loss_area as select a.date, a.area, ub_eggs_sold*b.procurement_cost as ub_FM_total_expense from ( select date, area, ub_eggs_sold from eggozdb.Maplemonk.Unbranded_loss_area )a left join ( select date_new, area, div0(fm_total_expense,total_eggs_procured) procurement_cost from eggozdb.Maplemonk.first_mile_new_area ) b on a.date=b.date_new and a.area=b.area ; create or replace table eggozdb.Maplemonk.ub_LM_loss_area as select a.area, a.date, sum(a.cost) ub_lm_total_expense from ( select \'NCR-MT\' as area, to_date(date,\'dd/mm/yyyy\') as date, sum(case when cost is null then 0 when cost = \'#REF!\' then 0 else replace(cost,\',\',\'\')::float end) + sum(case when unloading_cost is null then 0 when unloading_cost = \'#REF!\' then 0 else replace(unloading_cost,\',\',\'\')::float end) as cost from eggozdb.Maplemonk.transport_costs_last___mid_mile where \"GT/MT\" in (\'UB\') or beat in (\'Unbranded\') and cost <>\'#REF!\' group by date )a left join ( select date,case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end area, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by date,case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end )b on a.date=b.date and a.area=b.area group by a.area, a.date ; create or replace table eggozdb.Maplemonk.ub_sales_salary_new_area as select last_day(to_date(concat(ss.year,\'-\',ss.month,\'-\',\'01\'),\'yyyy-mmmm-dd\')) as month, \'NCR-MT\' as area, sum(replace(ss.\"Sales Salary\",\',\',\'\')) ub_sales_salary_total from eggozdb.Maplemonk.BP_Sales_Salary_by_area ss left join (select case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end area, last_day(date) as month, sum(net_sales) as net_sales, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return, sum(eggs_replaced) as eggs_replaced from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end, last_day(date))a on a.area = ss.classification and a.month = last_day(to_date(concat(ss.year,\'-\',ss.month,\'-\',\'01\'),\'yyyy-mmmm-dd\')) where ss.classification = \'NCR-UB\' group by ss.year,ss.month,ss.classification; create or replace table eggozdb.maplemonk.packaging_manpower_area as select date, l.area as area, manpower_cost_per_day/count(distinct l.area) over (partition by date) manpower_cost_per_day from (select last_day(to_date(p.month)) as month, date, Region as area, replace(\"Net Gurgaon EPC Salary\",\',\',\'\')::float/right(last_day(to_date(p.month)),2) manpower_cost_per_day from eggozdb.Maplemonk.BP_Pkg_Manpower p left join (select case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end area, date, last_day(date) as month from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku where beat_number_operations is not null and case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end in (\'Delhi-GT\', \'Gurgaon-GT\', \'Noida-GT\', \'NCR-MT\',\'NCR-HORECA\') group by case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end, last_day(date), date) k on (case when k.area in (\'Delhi-GT\', \'Gurgaon-GT\', \'Noida-GT\', \'NCR-MT\', \'NCR-HORECA\') then \'NCR\' end)= p.region and k.month = last_day(to_date(p.month)) group by region, last_day(to_date(p.month)), replace(\"Net Gurgaon EPC Salary\",\',\',\'\')::float,date) s left join (select distinct case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end area from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku where case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end in (\'Delhi-GT\', \'Gurgaon-GT\', \'Noida-GT\', \'NCR-MT\', \'NCR-HORECA\')) l on s.area = (case when l.area in (\'Delhi-GT\', \'Gurgaon-GT\', \'Noida-GT\', \'NCR-MT\',\'NCR-HORECA\') then \'NCR\' end) ; create or replace table eggozdb.maplemonk.promo_loss_area as select e.area, p.date, sum(e.promo_eggs_sold*p.cost_per_egg) as promo_expense from (select date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)) as date, case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end as type_new, sum(replace(amount,\',\',\'\')::float) as amount, sum(replace(eggs,\',\',\'\')::float) as eggs, div0(sum(replace(amount,\',\',\'\')::float),sum(replace(eggs,\',\',\'\')::float)) as cost_per_egg from eggozdb.Maplemonk.procurement_cost_procure where eggs not in (\'bill total is 78524\',\'GRN missed\',\'2 tray extra\',\'2 tray short\') group by date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)), case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end) p left join (select distinct date, area, type, promo_eggs_sold from eggozdb.Maplemonk.eggs_sold_new) e on p.date = e.date and p.type_new = e.type group by p.date, e.area order by p.date desc ; create or replace table eggozdb.Maplemonk.rent_cost_area as select last_day(to_date(concat(os.year,\'-\',os.month,\'-\',01),\'yyyy-mmmm-dd\')) as month, os.classification as area, sum(ifnull(replace(os.Rent,\',\',\'\')::float,0)) as rent_cost_total from eggozdb.Maplemonk.BP_Rent os group by os.year,os.month,os.classification; create or replace table eggozdb.maplemonk.total_eggs_loss_area as select to_date(a.date,\'dd/mm/yyyy\') date ,(case when \"PPP Loss(White)\" = \'#REF!\' then 0 else \"PPP Loss(White)\" end +\"Missing (White)\"+\"General Loss\"+\"UB Loss (White)\") + (case when \"PPP Loss(Brown)\" = \'#REF!\' then 0 else \"PPP Loss(Brown)\" end+\"Missing (Brown)\"+\"UB Loss (Brown)\") + (\"PPP Loss(Nutra)\"+\"Missing (Liquid)\"+\"UB Loss (Nutra)\") total_eggs ,(case when \"PPP Loss(White)\" = \'#REF!\' then 0 else \"PPP Loss(White)\" end+\"Missing (White)\"+\"General Loss\"+\"UB Loss (White)\")*cost_per_egg_White + (case when \"PPP Loss(Brown)\" = \'#REF!\' then 0 else \"PPP Loss(Brown)\" end+\"Missing (Brown)\"+\"UB Loss (Brown)\")*cost_per_egg_brown + (\"PPP Loss(Nutra)\"+\"Missing (Liquid)\"+\"UB Loss (Nutra)\")*cost_per_Egg_nutra as total_eggs_loss from eggozdb.maplemonk.loss_daily_loss_log a left join (select date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)) as date, div0(sum(case when type in (\'White\',\'white\') then replace(amount,\',\',\'\')::float end) ,sum(case when type in (\'White\',\'white\') then replace(eggs,\',\',\'\')::float end)) as cost_per_egg_white ,div0(sum(case when type in (\'Brown\',\'brown\') then replace(amount,\',\',\'\')::float end) ,sum(case when type in (\'Brown\',\'brown\') then replace(eggs,\',\',\'\')::float end)) as cost_per_egg_brown ,div0(sum(case when type in (\'Nutra Plus\',\'Nutra+\') then replace(amount,\',\',\'\')::float end),sum(case when type in (\'Nutra Plus\',\'Nutra+\') then replace(eggs,\',\',\'\')::float end)) as cost_per_egg_nutra from eggozdb.Maplemonk.procurement_cost_procure where eggs not in (\'bill total is 78524\',\'GRN missed\',\'2 tray extra\',\'2 tray short\') group by date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2))) p on p.date = to_date(a.date,\'dd/mm/yyyy\') where to_date(a.date,\'dd/mm/yyyy\') between \'2022-07-01\' and \'2022-07-31\' ; create or replace table eggozdb.Maplemonk.profit_area as select coalesce(a.area,m.area) area, coalesce(a.date,m.date) date, a.net_sales, a.amount_return, a.eggs_sold, a.eggs_return, a.eggs_replaced, a.eggs_promo, proc.Procurement_total_expense, fm.total_eggs_procured, fm.total_eggs_procured_ncr, fm.fm_total_expense, mm.mm_total_expense*(a.eggs_sold-a.eggs_return) mm_total_expense, lm.lm_total_expense, pack.packaging_total_expense as Packaging_total_expense_per_day, pack.packaging_total_expense_replaced as Packaging_total_expense_replaced_per_day, pack.packaging_total_expense_return as Packaging_total_expense_return_per_day, pack.packaging_total_expense_promo as Packaging_total_expense_promo_per_day, disc.Discount as discount_total, m.manpower_cost_per_day as Manpower_total_expense, ss.sales_salary_total/right(last_day(a.date),2) as Sales_Salary_total_per_day, os.ops_salary_total/right(last_day(a.date),2) as Ops_Salary_total_per_day, rc.RENT_COST_TOTAL/right(last_day(a.date),2) as Rent_total_expense, case when a.area = \'NCR-MT\' then a.net_Sales*0.15 else c.credit_loss_total end as credit_loss_total, ub.ub_loss_total_expense, ub.ub_eggs_sold, uf.ub_FM_total_expense, ul.ub_lm_total_expense, uss.ub_sales_salary_total/right(last_day(a.date),2) ub_sales_salary_total, pl.promo_expense, aod.sales_visits, aod.all_visits, aod.onboarded_as_of, ni.\"Opening Stock\" opening_stock, ni.\"Actual closing\" actual_stock, tel.total_eggs_loss from ( select case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end area, date, sum(net_sales) as net_sales, sum(amount_return) as amount_return, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return, sum(eggs_replaced) as eggs_replaced, sum(eggs_promo) as eggs_promo from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by case when area in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else area end,date )a left join (select distinct date,area,Procurement_total_expense from eggozdb.Maplemonk.procurement_new_area) proc on a.date=proc.date and a.area=proc.area left join (select distinct date_new,area,fm_total_expense,total_eggs_procured,total_eggs_procured_ncr from eggozdb.Maplemonk.first_mile_new_area) fm on a.date=fm.date_new and a.area=fm.area left join (select distinct date_new,area,mm_total_expense from eggozdb.Maplemonk.mid_mile_new_area) mm on a.date=mm.date_new and a.area=mm.area left join (select distinct date,area,lm_total_expense from eggozdb.Maplemonk.last_mile_new_area) lm on a.date=lm.date and a.area=lm.area left join (select distinct month,date, region,packaging_total_expense,packaging_total_expense_replaced,packaging_total_expense_return,packaging_total_expense_promo from eggozdb.Maplemonk.packaging_new_area) pack on a.date=pack.date and a.area=pack.region left join (select cast(timestampadd(minute,660,o.date) as date) as Date, case when rr.area_classification in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else rr.area_classification end as area, sum(o.scheme_discount_amount) as Discount from eggozdb.Maplemonk.my_sql_order_order o left join eggozdb.Maplemonk.my_sql_retailer_retailer rr ON o.retailer_id =rr.id group by cast(timestampadd(minute,660,o.date) as date), case when rr.area_classification in (\'NCR-ON-MT\',\'NCR-OF-MT\') then \'NCR-MT\' else rr.area_classification end) disc on a.date=disc.date and a.area=disc.area right join eggozdb.maplemonk.packaging_manpower_area m on a.date=m.date and a.area=m.area left join eggozdb.Maplemonk.sales_salary_new_area ss on last_day(a.date)=ss.month and a.area=ss.area left join eggozdb.Maplemonk.ops_salary_new_area os on last_day(a.date)=os.month and a.area=os.area left join eggozdb.maplemonk.credit_loss_area c on a.date=c.date and a.area=c.area left join eggozdb.Maplemonk.Unbranded_loss_area ub on a.date=ub.date and a.area=ub.area left join eggozdb.maplemonk.promo_loss_area pl on a.date=pl.date and a.area=pl.area left join eggozdb.Maplemonk.rent_cost_area rc on last_day(a.date)=rc.month and rc.area = a.area left join (select date, area, sum(sales_visits) sales_visits, sum(all_visits) all_visits, sum(onboarded_As_of) onboarded_as_of from eggozdb.maplemonk.Retailer_onboarded_as_of_date group by 1, 2) aod on aod.date = a.date and aod.area = a.area left join eggozdb.maplemonk.ncr_inventory ni on try_to_date(ni.date,\'dd/mm/yyyy\') = a.date left join eggozdb.maplemonk.ub_FM_loss_area uf on uf.area = a.area and uf.date = a.date left join eggozdb.Maplemonk.ub_LM_loss_area ul on ul.date = a.date and ul.area=a.area left join eggozdb.Maplemonk.ub_sales_salary_new_area uss on last_day(a.date)=uss.month and uss.area = a.area left join (select date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)) as date ,sum(replace(eggs,\',\',\'\')::float) as total_eggs_procured ,sum(replace(eggs,\',\',\'\')::float)/5 as total_eggs_procured_ncr from eggozdb.Maplemonk.procurement_cost_procure where eggs not in (\'bill total is 78524\',\'GRN missed\',\'2 tray extra\',\'2 tray short\') group by date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)) ) ep on ep.date=a.date left join eggozdb.maplemonk.total_eggs_loss_area tel on tel.date = a.date ;",
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
                        