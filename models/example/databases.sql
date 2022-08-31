{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.Maplemonk.eggs_sold_new as with cte as (select date, area, beat_new, sku, \'White\' as Type, sum(ifnull(eggs_sold_white,0))- sum(ifnull(eggs_return_white,0)) as net_eggs_sold, sum(ifnull(eggs_replaced_white,0)) + sum(ifnull(eggs_return_white,0)) as UB_eggs_sold, sum(ifnull(eggs_promo_white,0)) as promo_eggs_sold from (select *, case when beat_number_operations=0 then 9999 else beat_number_operations end as beat_new from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku) group by date, area, beat_new, sku UNION select date, area, beat_new, sku, \'Brown\' as Type, sum(ifnull(eggs_sold_brown,0))- sum(ifnull(eggs_return_brown,0)) as net_eggs_sold, sum(ifnull(eggs_replaced_brown,0)) + sum(ifnull(eggs_return_brown,0)) as UB_eggs_sold, sum(ifnull(eggs_promo_brown,0)) as promo_eggs_sold from (select *, case when beat_number_operations=0 then 9999 else beat_number_operations end as beat_new from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku) group by date, area, beat_new, sku UNION select date, area, beat_new, sku, \'Nutra Plus\' as Type, sum(ifnull(eggs_sold_nutra,0)) - sum(ifnull(eggs_return_nutra,0)) as net_eggs_sold, sum(ifnull(eggs_replaced_nutra,0)) + sum(ifnull(eggs_return_nutra,0)) as UB_eggs_sold, sum(ifnull(eggs_promo_nutra,0)) as promo_eggs_sold from (select *, case when beat_number_operations=0 then 9999 else beat_number_operations end as beat_new from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku) group by date, area, beat_new, sku ) select date, area, beat_new, sku, type, net_eggs_sold, UB_eggs_sold, promo_eggs_sold from cte; create or replace table eggozdb.Maplemonk.procurement_new as select p.date, e.area, e.beat_new, sum(case when lower(p.type_new) = \'white\' then e.net_eggs_sold * (p.cost_per_egg + 0.11) when lower(p.type_new) = \'brown\' then e.net_eggs_sold * (p.cost_per_egg + 0.11) when lower(p.type_new) = \'nutra plus\' then e.net_eggs_sold * (p.cost_per_egg + 0.45) end ) as Procurement_total_expense from (select date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)) as date, case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end as type_new, sum(replace(amount,\',\',\'\')::float) as amount, sum(replace(eggs,\',\',\'\')::float) as eggs, div0(sum(replace(amount,\',\',\'\')::float),sum(replace(eggs,\',\',\'\')::float)) as cost_per_egg from eggozdb.Maplemonk.procurement_cost_procure where eggs not in (\'bill total is 78524\',\'GRN missed\',\'2 tray extra\',\'2 tray short\') group by date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)), case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end) p left join (select distinct date, area, beat_new, type, net_eggs_sold from eggozdb.Maplemonk.eggs_sold_new) e on p.date = e.date and p.type_new = e.type group by p.date, e.area, e.beat_new order by p.date desc; create or replace table eggozdb.Maplemonk.first_mile_new as select a.date_new, s.area, s.beat, div0(a.fm_total_expense,count(distinct area,beat) over (partition by date)) fm_total_expense, div0(a.total_eggs_procured, count(distinct area,beat) over (partition by date)) total_eggs_procured from(select fm.date_new, sum(fm.total_expense) fm_total_expense, sum(p.total_eggs) as total_eggs_procured from (select to_date(date,\'DD/MM/YYYY\') as date_new, sum(total_expense) as total_expense from eggozdb.Maplemonk.transport_costs_fm_vehicle_details_after_1st_april group by to_date(date,\'DD/MM/YYYY\')) fm left join (select date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)) as date, sum(replace(eggs,\',\',\'\')::float) as total_eggs from eggozdb.Maplemonk.procurement_cost_procure where eggs not in (\'bill total is 78524\',\'GRN missed\',\'2 tray extra\',\'2 tray short\') group by date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)))p on fm.date_new = p.date group by fm.date_new) a left join (select date, area, beat_number_operations as beat, sum(eggs_sold) as eggs_sold, sum(eggs_replaced) as eggs_replaced, sum(eggs_return) as eggs_return from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by date,area,beat_number_operations) s on a.date_new=s.date ; create or replace table eggozdb.Maplemonk.mid_mile_new as select a.date_new, b.area, div0(a.mm_total_expense,b.net_eggs) as mm_total_expense from (select to_date(date,\'dd/mm/yyyy\') as date_new, case when area = \'Delhi\' then \'Delhi-GT\' when area = \'Gurgaon\' then \'Gurgaon-GT\' when area = \'Noida\' then \'Noida-GT\' end as area, sum(case when cost is null then 0 else replace(cost,\',\',\'\')::float end) as mm_total_expense, sum(case when total_eggs_out is null then 0 else total_eggs_out::float end) as total_eggs_out from eggozdb.Maplemonk.transport_costs_last___mid_mile where \"GT/MT\" in (\'MM\',\'WMT\') and cost not in (\'#REF!\',\'#N/A\') group by to_date(date,\'dd/mm/yyyy\'), area )a left join (select date, area, sum(eggs_sold - eggs_return) net_eggs from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_dso where area in (\'Delhi-GT\',\'Noida-GT\',\'NCR-MT\',\'NCR-HORECA\',\'Gurgaon-GT\') and lower(operator) <> \'rajeev\' group by area, date )b on a.date_new=b.date and a.area = b.area ; create or replace table eggozdb.Maplemonk.last_mile_new as select a.area, a.beat, a.date, a.cost cost, b.eggs_sold eggs_sold, b.eggs_return eggs_return, a.cost lm_total_expense from ( select case when \"GT/MT\" in (\'LMT\',\'OFF-MT\',\'ON-MT\') then \'NCR-MT\' when \"GT/MT\"=\'GGT\' then \'Gurgaon-GT\' when \"GT/MT\"=\'DGT\' then \'Delhi-GT\' when \"GT/MT\"=\'NGT\' then \'Noida-GT\' when \"GT/MT\" = \'HORECA\' then \'NCR-HORECA\' end as area, case when beat like \'%hoc%\' or beat like \'%DHOC%\' or beat like \'%Horeca%\' then 9999 else beat end as beat, to_date(date,\'dd/mm/yyyy\') as date, sum(case when cost is null then 0 else replace(cost,\',\',\'\')::float end) + sum(case when unloading_cost is null then 0 else replace(unloading_cost,\',\',\'\')::float end) as cost from eggozdb.Maplemonk.transport_costs_last___mid_mile where \"GT/MT\" in (\'LMT\',\'GGT\',\'DGT\',\'NGT\',\'OFF-MT\',\'ON-MT\',\'HORECA\') and cost not in (\'#REF!\',\'#N/A\') and beat not in (\'Refilling\',\'RTV\',\'Unbranded\') group by case when beat like \'%hoc%\' or beat like \'%DHOC%\' or beat like \'%Horeca%\' then 9999 else beat end, date, case when \"GT/MT\" in (\'LMT\',\'OFF-MT\',\'ON-MT\') then \'NCR-MT\' when \"GT/MT\"=\'GGT\' then \'Gurgaon-GT\' when \"GT/MT\"=\'DGT\' then \'Delhi-GT\' when \"GT/MT\"=\'NGT\' then \'Noida-GT\' when \"GT/MT\" = \'HORECA\' then \'NCR-HORECA\' end )a left join ( select date,area, case when beat_number_operations=0 then 9999 else beat_number_operations end as beat, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by date,area,beat_number_operations )b on a.beat=b.beat and a.date=b.date and a.area=b.area ; create or replace table eggozdb.Maplemonk.packaging_new as select month, date, region, beat, sum(weighted) as packaging_total_expense, sum(weighted_replaced) as packaging_total_expense_replaced, sum(weighted_return) as packaging_total_expense_return, sum(weighted_promo) as packaging_total_expense_promo from ( select a.month, b.date, a.region, a.sku, b.beat, a.cost, b.eggs_sold, b.eggs_replaced, b.eggs_return, a.cost*(b.eggs_sold-eggs_return) as weighted, a.cost*b.eggs_replaced as weighted_replaced, a.cost*b.eggs_return as weighted_return, a.cost*b.eggs_promo as weighted_promo from ( select region, sku, last_day(to_date(month,\'yyyy/mm/dd\')) as month, sum(packaging_cost) as cost from eggozdb.Maplemonk.packaging_cost_per_sku_per_egg group by region, sku, last_day(to_date(month,\'yyyy/mm/dd\')) )a left join ( select last_day(date) as month, date, sku, area as region, case when beat_number_operations IN (0,9999) then 9999 else beat_number_operations end as beat, sum(eggs_sold) as eggs_sold, sum(eggs_replaced) as eggs_replaced, sum(eggs_return) as eggs_return, sum(eggs_promo) as eggs_promo from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by last_day(date),sku ,date ,area ,case when beat_number_operations IN (0,9999) then 9999 else beat_number_operations end )b on a.sku=b.sku and a.month=b.month and a.region=b.region ) group by month,beat,region,date ; create or replace table eggozdb.maplemonk.credit_loss as select a.date, a.area, a.beat_number_operations, a.credit_loss as credit_loss_total from ( select p.transaction_date::date as date, rr.area_classification as area, case when ba.beat_number=0 then 9999 else ba.beat_number end as beat_number_operations, sum(p.transaction_amount) as credit_loss from eggozdb.maplemonk.my_sql_payment_salestransaction p left join eggozdb.Maplemonk.my_sql_retailer_retailer rr ON p.retailer_id =rr.id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = p.beat_assignment_id where transaction_type in (\'Credit Note\',\'Debit Note\') group by p.transaction_date::date, rr.area_classification, case when ba.beat_number=0 then 9999 else ba.beat_number end )a left join ( select area, case when beat_number_operations=0 then 9999 else beat_number_operations end as beat, date, sum(net_sales) as net_sales, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return, sum(eggs_replaced) as eggs_replaced from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku where beat_number_operations is not null group by area,case when beat_number_operations=0 then 9999 else beat_number_operations end,date )b on a.date=b.date and a.area=b.area and a.beat_number_operations=b.beat; create or replace table eggozdb.Maplemonk.sales_salary_new as select last_day(to_date(concat(ss.year,\'-\',ss.month,\'-\',01),\'yyyy-mmmm-dd\')) as month, ss.classification as area, ss.beat, sum(replace(ss.\"Sales Salary\",\',\',\'\')) sales_salary_total from eggozdb.Maplemonk.BP_Sales_Salary ss left join (select area, case when beat_number_operations=0 then 9999 else beat_number_operations end as beat, last_day(date) as month, sum(net_sales) as net_sales, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return, sum(eggs_replaced) as eggs_replaced from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku where beat_number_operations is not null group by area, beat_number_operations, last_day(date))a on a.area = ss.classification and a.beat=ss.beat and a.month = last_day(to_date(concat(ss.year,\'-\',ss.month,\'-\',01),\'yyyy-mmmm-dd\')) where ss.classification not in (\'NCR-UB\') group by ss.year,ss.month,ss.classification,ss.beat; create or replace table eggozdb.Maplemonk.ops_salary_new as select last_day(to_date(concat(os.year,\'-\',os.month,\'-\',01),\'yyyy-mmmm-dd\')) as month, os.classification as area, div0(sum(ifnull(replace(os.\"Final Ops Salary\",\',\',\'\')::float,0)),sum(ifnull(eggs_sold,0))+sum(ifnull(eggs_replaced,0))) as ops_salary_total from eggozdb.Maplemonk.BP_Ops_Salary os left join (select area, last_day(date) as month, sum(net_sales) as net_sales, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return, sum(eggs_replaced) as eggs_replaced from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku where beat_number_operations is not null group by area, last_day(date))a on a.month = last_day(to_date(concat(os.year,\'-\',os.month,\'-\',01),\'yyyy-mmmm-dd\')) and a.area=os.classification group by os.year,os.month,os.classification; create or replace table eggozdb.Maplemonk.Unbranded_loss as select m.date, e.area, ifnull(e.beat_new ,9999) beat, sum((m.cost_per_egg - \"Selling Price/Egg\")*e.ub_eggs_sold) as UB_Loss_total_expense, sum(e.ub_eggs_sold) as ub_eggs_sold from ( select p.date, ub.type_new, ub.\"Selling Price/Egg\", p.cost_per_egg from (select distinct deliverydate date, case when category=\'Nutra\' then \'Nutra Plus\' when category = \'Melted\' then \'White\' else category end as type_new, div0(sum(amount),sum(eggs_sold)) \"Selling Price/Egg\" from eggozdb.maplemonk.ub_log where area_classification = \'NCR-UB\' group by 1,2) ub left join( select date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)) as date, case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end as type, div0(sum(replace(amount,\',\',\'\')::float),sum(replace(eggs,\',\',\'\')::float)) as cost_per_egg from eggozdb.Maplemonk.procurement_cost_procure where eggs not in (\'bill total is 78524\',\'GRN missed\',\'2 tray extra\',\'2 tray short\') group by date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)), case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end) p on date_trunc(month, p.date) = ub.date and p.type = ub.type_new)m left join (select distinct date, area, beat_new, sum(ub_eggs_sold) ub_eggs_sold, type from eggozdb.Maplemonk.eggs_sold_new where area in (\'NCR-MT\',\'Delhi-GT\',\'Noida-GT\',\'Gurgaon-GT\',\'NCR-HORECA\') group by 1,2,3,5) e on m.date = e.date and m.type_new = e.type group by m.date, e.area, e.beat_new order by m.date desc, e.area desc,e.beat_new desc ; create or replace table eggozdb.maplemonk.ub_FM_loss as select a.date, a.area, a.beat, ub_eggs_sold*b.procurement_cost as ub_FM_total_expense from ( select date, area, beat, ub_eggs_sold from eggozdb.Maplemonk.Unbranded_loss )a left join ( select date_new, area, beat, div0(fm_total_expense,total_eggs_procured) procurement_cost from eggozdb.Maplemonk.first_mile_new ) b on a.date=b.date_new and a.area=b.area and a.beat=b.beat ; create or replace table eggozdb.Maplemonk.ub_LM_loss as select a.area, a.date, a.beat, sum(a.cost) ub_lm_total_expense from ( select \'NCR-UB\' as area, to_date(date,\'dd/mm/yyyy\') as date, case when beat like \'%hoc%\' or beat like \'%DHOC%\' or beat like \'%Horeca%\' or beat like \'%Unbranded%\' or beat like \'%Loose Tray%\' then 9999 else beat end as beat, sum(case when cost is null then 0 when cost = \'#REF!\' then 0 else replace(cost,\',\',\'\')::float end) + sum(case when unloading_cost is null then 0 when unloading_cost = \'#REF!\' then 0 else replace(unloading_cost,\',\',\'\')::float end) as cost from eggozdb.Maplemonk.transport_costs_last___mid_mile where \"GT/MT\" in (\'UB\') or beat in (\'Unbranded\') and cost <>\'#REF!\' group by date, case when beat like \'%hoc%\' or beat like \'%DHOC%\' or beat like \'%Horeca%\' or beat like \'%Unbranded%\' or beat like \'%Loose Tray%\' then 9999 else beat end )a left join ( select date,area,beat_number_operations beat, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by date,area,beat_number_operations )b on a.date=b.date and a.area=b.area and a.beat=b.beat group by a.area, a.date, a.beat ; create or replace table eggozdb.Maplemonk.ub_sales_salary_new as select last_day(to_date(concat(ss.year,\'-\',ss.month,\'-\',\'01\'),\'yyyy-mmmm-dd\')) as month, ss.classification as area, ifnull(a.beat,9999) beat, sum(replace(ss.\"Sales Salary\",\',\',\'\')) ub_sales_salary_total from eggozdb.Maplemonk.BP_Sales_Salary_by_area ss left join (select area, beat_number_operations beat, last_day(date) as month, sum(net_sales) as net_sales, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return, sum(eggs_replaced) as eggs_replaced from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by area, beat_number_operations, last_day(date))a on a.area = ss.classification and a.month = last_day(to_date(concat(ss.year,\'-\',ss.month,\'-\',\'01\'),\'yyyy-mmmm-dd\')) where ss.classification = \'NCR-UB\' group by ss.year,ss.month,ss.classification,a.beat; create or replace table eggozdb.maplemonk.promo_loss as select e.area, p.date, e.beat_new beat, sum(e.promo_eggs_sold*p.cost_per_egg) as promo_expense from (select date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)) as date, case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end as type_new, sum(replace(amount,\',\',\'\')::float) as amount, sum(replace(eggs,\',\',\'\')::float) as eggs, div0(sum(replace(amount,\',\',\'\')::float),sum(replace(eggs,\',\',\'\')::float)) as cost_per_egg from eggozdb.Maplemonk.procurement_cost_procure where eggs not in (\'bill total is 78524\',\'GRN missed\',\'2 tray extra\',\'2 tray short\') group by date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)), case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end) p left join (select distinct date, area, beat_new, type, promo_eggs_sold from eggozdb.Maplemonk.eggs_sold_new) e on p.date = e.date and p.type_new = e.type group by p.date, e.area, e.beat_new order by p.date desc ; create or replace table eggozdb.maplemonk.total_eggs_loss as select to_date(a.date,\'dd/mm/yyyy\') date ,(case when \"PPP Loss(White)\" = \'#REF!\' then 0 else \"PPP Loss(White)\" end+\"Missing (White)\"+\"General Loss\"+\"UB Loss (White)\") + (case when \"PPP Loss(Brown)\" = \'#REF!\' then 0 else \"PPP Loss(Brown)\" end +\"Missing (Brown)\"+\"UB Loss (Brown)\") + (\"PPP Loss(Nutra)\"+\"Missing (Liquid)\"+\"UB Loss (Nutra)\") total_eggs ,(case when \"PPP Loss(White)\" = \'#REF!\' then 0 else \"PPP Loss(White)\" end +\"Missing (White)\"+\"General Loss\"+\"UB Loss (White)\")*cost_per_egg_White + (case when \"PPP Loss(Brown)\" = \'#REF!\' then 0 else \"PPP Loss(Brown)\" end +\"Missing (Brown)\"+\"UB Loss (Brown)\")*cost_per_egg_brown + (\"PPP Loss(Nutra)\"+\"Missing (Liquid)\"+\"UB Loss (Nutra)\")*cost_per_Egg_nutra as total_eggs_loss from eggozdb.maplemonk.loss_daily_loss_log a left join (select date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2)) as date, div0(sum(case when type in (\'White\',\'white\') then replace(amount,\',\',\'\')::float end) ,sum(case when type in (\'White\',\'white\') then replace(eggs,\',\',\'\')::float end)) as cost_per_egg_white ,div0(sum(case when type in (\'Brown\',\'brown\') then replace(amount,\',\',\'\')::float end) ,sum(case when type in (\'Brown\',\'brown\') then replace(eggs,\',\',\'\')::float end)) as cost_per_egg_brown ,div0(sum(case when type in (\'Nutra Plus\',\'Nutra+\') then replace(amount,\',\',\'\')::float end),sum(case when type in (\'Nutra Plus\',\'Nutra+\') then replace(eggs,\',\',\'\')::float end)) as cost_per_egg_nutra from eggozdb.Maplemonk.procurement_cost_procure where eggs not in (\'bill total is 78524\',\'GRN missed\',\'2 tray extra\',\'2 tray short\') group by date_from_parts(concat(\'20\',right(\"GRN Date\",2)),left(right(\"GRN Date\",5),2),left(\"GRN Date\",2))) p on p.date = to_date(a.date,\'dd/mm/yyyy\') ; create or replace table eggozdb.Maplemonk.profit as select a.area, a.date, a.beat, a.net_sales, a.amount_return, a.eggs_sold, a.eggs_return, a.eggs_replaced, a.eggs_promo, proc.Procurement_total_expense, fm.total_eggs_procured, fm.fm_total_expense, mm.mm_total_expense*(a.eggs_sold-a.eggs_return) mm_total_expense, lm.lm_total_expense, pack.packaging_total_expense as Packaging_total_expense_per_day, pack.packaging_total_expense_replaced as Packaging_total_expense_replaced_per_day, pack.packaging_total_expense_return as Packaging_total_expense_return_per_day, pack.packaging_total_expense_promo as Packaging_total_expense_promo_per_day, disc.Discount as discount_total, (replace(bpm.\"Net Gurgaon EPC Salary\",\',\',\'\')/right(last_day(a.date),2))*div0((a.eggs_sold - a.eggs_return),sum(a.eggs_sold - a.eggs_return) over (partition by a.date)) manpower_total_expense, ss.sales_salary_total/right(last_day(a.date),2) as Sales_Salary_total_per_day, os.ops_salary_total*(ifnull(a.eggs_sold,0) + ifnull(a.eggs_replaced,0)) as Ops_Salary_total_per_day, (ifnull(replace(bpr.Rent,\',\',\'\')::float,0)/right(last_day(a.date),2))*div0((a.eggs_sold - a.eggs_return),sum(a.eggs_sold - a.eggs_return) over (partition by a.date,a.area)) Rent_total_expense, case when a.area = \'NCR-MT\' then a.net_Sales*0.15 else c.credit_loss_total end as credit_loss_total, ub.ub_loss_total_expense, ub.ub_eggs_sold, uf.ub_FM_total_expense, ul.ub_lm_total_expense, uss.ub_sales_salary_total, pl.promo_expense, aod.sales_visits, aod.all_visits, aod.onboarded_as_of, tel.total_eggs_loss from ( select area, date, case when beat_number_operations=0 then 9999 when beat_number_operations is null then 9999 else beat_number_operations end as beat, sum(net_sales) as net_sales, sum(amount_return) as amount_return, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return, sum(eggs_replaced) as eggs_replaced, sum(eggs_promo) as eggs_promo from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by area,date ,case when beat_number_operations=0 then 9999 when beat_number_operations is null then 9999 else beat_number_operations end )a left join (select distinct date,area,beat_new,Procurement_total_expense from eggozdb.Maplemonk.procurement_new) proc on a.date=proc.date and a.beat=proc.beat_new and a.area=proc.area left join (select distinct date_new,area,beat,fm_total_expense,total_eggs_procured from eggozdb.Maplemonk.first_mile_new) fm on a.date=fm.date_new and a.beat=fm.beat and a.area=fm.area left join (select distinct date_new,area,mm_total_expense from eggozdb.Maplemonk.mid_mile_new) mm on a.date=mm.date_new and a.area=mm.area left join (select distinct date,beat,area,lm_total_expense from eggozdb.Maplemonk.last_mile_new) lm on a.date=lm.date and a.beat=lm.beat and a.area=lm.area left join (select distinct month,date, beat,region,packaging_total_expense,packaging_total_expense_replaced,packaging_total_expense_return,packaging_total_expense_promo from eggozdb.Maplemonk.packaging_new) pack on a.date=pack.date and a.beat=pack.beat and a.area=pack.region left join (select cast(timestampadd(minute,660,o.date) as date) as Date, rr.area_classification as area, ba.beat_number as beat_number_operations, sum(o.scheme_discount_amount) as Discount from eggozdb.Maplemonk.my_sql_order_order o left join eggozdb.Maplemonk.my_sql_retailer_retailer rr ON o.retailer_id =rr.id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = o.beat_assignment_id group by cast(timestampadd(minute,660,o.date) as date), rr.area_classification, ba.beat_number) disc on a.date=disc.date and a.beat=disc.beat_number_operations and a.area=disc.area left join eggozdb.Maplemonk.sales_salary_new ss on last_day(a.date)=ss.month and a.area=ss.area and a.beat=ss.beat left join eggozdb.Maplemonk.ops_salary_new os on last_day(a.date)=os.month and a.area=os.area left join eggozdb.maplemonk.credit_loss c on a.date=c.date and a.area=c.area and a.beat=c.beat_number_operations left join eggozdb.Maplemonk.Unbranded_loss ub on a.date=ub.date and a.area=ub.area and a.beat = ub.beat left join eggozdb.maplemonk.promo_loss pl on a.date=pl.date and a.area=pl.area and a.beat=pl.beat left join eggozdb.maplemonk.Retailer_onboarded_as_of_date aod on aod.date = a.date and aod.area = a.area and aod.beat_number_original = a.beat left join eggozdb.maplemonk.ub_FM_loss uf on uf.area = a.area and uf.date = a.date and uf.beat=a.beat left join eggozdb.Maplemonk.ub_LM_loss ul on ul.date = a.date and ul.area=a.area and ul.beat=a.beat left join eggozdb.Maplemonk.ub_sales_salary_new uss on last_day(a.date)=uss.month and uss.area = a.area and uss.beat = a.beat left join eggozdb.maplemonk.total_eggs_loss_area tel on tel.date = a.date left join eggozdb.Maplemonk.BP_Pkg_Manpower bpm on (case when a.area in (\'Delhi-GT\', \'Gurgaon-GT\', \'Noida-GT\', \'NCR-MT\',\'NCR-HORECA\') then \'NCR\' end)= bpm.region and last_day(a.date) = last_day(to_date(bpm.month)) left join eggozdb.Maplemonk.BP_Rent bpr on bpr.classification = a.area and last_day(to_date(concat(bpr.year,\'-\',bpr.month,\'-\',01),\'yyyy-mmmm-dd\')) = last_day(a.date) where a.area in (\'NCR-MT\', \'Gurgaon-GT\',\'Noida-GT\',\'Delhi-GT\',\'NCR-HORECA\') ;",
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
                        