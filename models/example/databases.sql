{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.Maplemonk.eggs_sold_new as with cte as (select date, area, beat_new, sku, \'White\' as Type, sum(ifnull(eggs_sold_white,0))- sum(ifnull(eggs_return_white,0)) as net_eggs_sold, sum(ifnull(eggs_replaced_white,0)) + sum(ifnull(eggs_return_white,0)) as UB_eggs_sold, sum(ifnull(eggs_promo_white,0)) as promo_eggs_sold from (select *, case when beat_number_operations=0 then 9999 else beat_number_operations end as beat_new from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku) group by date, area, beat_new, sku UNION select date, area, beat_new, sku, \'Brown\' as Type, sum(ifnull(eggs_sold_brown,0))- sum(ifnull(eggs_return_brown,0)) as net_eggs_sold, sum(ifnull(eggs_replaced_brown,0)) + sum(ifnull(eggs_return_brown,0)) as UB_eggs_sold, sum(ifnull(eggs_promo_brown,0)) as promo_eggs_sold from (select *, case when beat_number_operations=0 then 9999 else beat_number_operations end as beat_new from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku) group by date, area, beat_new, sku UNION select date, area, beat_new, sku, \'Nutra Plus\' as Type, sum(ifnull(eggs_sold_nutra,0)) - sum(ifnull(eggs_return_nutra,0)) as net_eggs_sold, sum(ifnull(eggs_replaced_nutra,0)) + sum(ifnull(eggs_return_nutra,0)) as UB_eggs_sold, sum(ifnull(eggs_promo_nutra,0)) as promo_eggs_sold from (select *, case when beat_number_operations=0 then 9999 else beat_number_operations end as beat_new from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku) group by date, area, beat_new, sku ) select date, area, beat_new, sku, type, net_eggs_sold, UB_eggs_sold, promo_eggs_sold from cte; create or replace table eggozdb.Maplemonk.procurement_new as select p.date, e.area, e.beat_new, sum(e.net_eggs_sold * p.cost_per_egg) as Procurement_total_expense from (select date_from_parts(year,month,day) as date, case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end as type_new, sum(replace(amount,\',\',\'\')::float) as amount, sum(replace(eggs,\',\',\'\')::float) as eggs, div0(sum(replace(amount,\',\',\'\')::float),sum(replace(eggs,\',\',\'\')::float)) as cost_per_egg from eggozdb.Maplemonk.procurement_cost_procure where eggs not in (\'bill total is 78524\',\'GRN missed\',\'2 tray extra\',\'2 tray short\') group by date_from_parts(year,month,day), case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end) p left join (select distinct date, area, beat_new, type, net_eggs_sold from eggozdb.Maplemonk.eggs_sold_new) e on p.date = e.date and p.type_new = e.type group by p.date, e.area, e.beat_new order by p.date desc; create or replace table eggozdb.Maplemonk.first_mile_new as select a.date_new, s.area, s.beat, a.fm_total_expense, a.total_eggs_procured from(select fm.date_new, sum(fm.total_expense) fm_total_expense, sum(p.total_eggs) as total_eggs_procured from (select to_date(date,\'DD/MM/YYYY\') as date_new, sum(total_expense) as total_expense from eggozdb.Maplemonk.transport_costs_fm_vehicle_details_after_1st_april group by to_date(date,\'DD/MM/YYYY\')) fm left join (select date_from_parts(year,month,day) as date, sum(replace(eggs,\',\',\'\')::float) as total_eggs from eggozdb.Maplemonk.procurement_cost_procure where eggs not in (\'bill total is 78524\',\'GRN missed\',\'2 tray extra\',\'2 tray short\') group by date_from_parts(year,month,day))p on fm.date_new = p.date group by fm.date_new) a left join (select date, area, beat_number_operations as beat, sum(eggs_sold) as eggs_sold, sum(eggs_replaced) as eggs_replaced, sum(eggs_return) as eggs_return from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by date,area,beat_number_operations) s on a.date_new=s.date ; create or replace table eggozdb.Maplemonk.mid_mile_new as select a.date_new, b.area, b.beat_new, a.mm_total_expense_per_egg*(ifnull(b.eggs_sold,0)+ifnull(b.eggs_replaced,0)) as mm_total_expense from(select mm.date_new, div0(mm.cost_new,mm.total_eggs_out) as mm_total_expense_per_egg from(select to_date(date,\'dd-mon-yyyy\') as date_new, sum(case when cost is null then 0 else replace(cost,\',\',\'\')::float end) as cost_new, sum(case when total_eggs_out is null then 0 else total_eggs_out::float end) as total_eggs_out from eggozdb.Maplemonk.transport_costs_last___mid_mile where \"GT/MT\" in (\'MM\',\'WMT\') and cost not in (\'#REF!\',\'#N/A\') group by to_date(date,\'dd-mon-yyyy\')) mm )a left join (select date, area, case when beat_number_operations=0 then 9999 else beat_number_operations end as beat_new, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku where area in (\'Delhi-GT\',\'Noida-GT\') group by area, date, Beat_number_operations )b on a.date_new=b.date ; create or replace table eggozdb.Maplemonk.last_mile_new as select a.area, a.beat, a.date, a.cost, b.eggs_sold, b.eggs_return, a.cost lm_total_expense from ( select case when \"GT/MT\"=\'LMT\' then \'NCR-MT\' when \"GT/MT\"=\'GGT\' then \'Gurgaon-GT\' when \"GT/MT\"=\'DGT\' then \'Delhi-GT\' when \"GT/MT\"=\'NGT\' then \'Noida-GT\' end as area, case when beat like \'%hoc%\' or beat like \'%DHOC%\' then 9999 else beat end as beat, to_date(date,\'dd-mon-yyyy\') as date, sum(case when cost is null then 0 else replace(cost,\',\',\'\')::float end) as cost from eggozdb.Maplemonk.transport_costs_last___mid_mile where \"GT/MT\" in (\'LMT\',\'GGT\',\'DGT\',\'NGT\') and cost not in (\'#REF!\',\'#N/A\') and beat not in (\'Refilling\',\'RTV\') group by case when beat like \'%hoc%\' or beat like \'%DHOC%\' then 9999 else beat end, date, case when \"GT/MT\"=\'LMT\' then \'NCR-MT\' when \"GT/MT\"=\'GGT\' then \'Gurgaon-GT\' when \"GT/MT\"=\'DGT\' then \'Delhi-GT\' when \"GT/MT\"=\'NGT\' then \'Noida-GT\' end )a left join ( select date,area, case when beat_number_operations=0 then 9999 else beat_number_operations end as beat, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by date,area,beat_number_operations )b on a.beat=b.beat and a.date=b.date and a.area=b.area ; create or replace table eggozdb.Maplemonk.packaging_new as select month, date, region, beat, sum(weighted) as packaging_total_expense, sum(weighted_replaced) as packaging_total_expense_replaced, sum(weighted_return) as packaging_total_expense_return, sum(weighted_promo) as packaging_total_expense_promo from ( select a.month, b.date, a.region, a.sku, b.beat, a.cost, b.eggs_sold, b.eggs_replaced, b.eggs_return, a.cost*(b.eggs_sold-eggs_return) as weighted, a.cost*b.eggs_replaced as weighted_replaced, a.cost*b.eggs_return as weighted_return, a.cost*b.eggs_promo as weighted_promo from ( select region, sku, last_day(to_date(month,\'yyyy/mm/dd\')) as month, sum(packaging_cost) as cost from eggozdb.Maplemonk.packaging_cost_per_sku_per_egg group by region, sku, last_day(to_date(month,\'yyyy/mm/dd\')) )a left join ( select last_day(date) as month, date, sku, left(area,charindex(\'-\',area)-1) as region, case when beat_number_operations IN (0,9999) then 9999 else beat_number_operations end as beat, sum(eggs_sold) as eggs_sold, sum(eggs_replaced) as eggs_replaced, sum(eggs_return) as eggs_return, sum(eggs_promo) as eggs_promo from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku group by last_day(date),sku ,date ,left(area,charindex(\'-\',area)-1) ,case when beat_number_operations IN (0,9999) then 9999 else beat_number_operations end )b on a.sku=b.sku and a.month=b.month and a.region=b.region ) group by month,beat,region,date ; create or replace table eggozdb.maplemonk.credit_loss as select a.date, a.area, a.beat_number_operations, a.credit_loss as credit_loss_total from ( select p.transaction_date::date as date, rr.area_classification as area, case when ba.beat_number=0 then 9999 else ba.beat_number end as beat_number_operations, sum(p.transaction_amount) as credit_loss from eggozdb.maplemonk.my_sql_payment_salestransaction p left join eggozdb.Maplemonk.my_sql_retailer_retailer rr ON p.retailer_id =rr.id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = p.beat_assignment_id where transaction_type in (\'Credit Note\',\'Debit Note\') group by p.transaction_date::date, rr.area_classification, case when ba.beat_number=0 then 9999 else ba.beat_number end )a left join ( select area, case when beat_number_operations=0 then 9999 else beat_number_operations end as beat, date, sum(net_sales) as net_sales, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return, sum(eggs_replaced) as eggs_replaced from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku where beat_number_operations is not null group by area,case when beat_number_operations=0 then 9999 else beat_number_operations end,date )b on a.date=b.date and a.area=b.area and a.beat_number_operations=b.beat; create or replace table eggozdb.Maplemonk.sales_salary_new as select last_day(to_date(concat(ss.year,\'-\',ss.month,\'-\',01),\'yyyy-mmmm-dd\')) as month, ss.classification as area, ss.beat, sum(ss.\"Sales Salary\") sales_salary_total from eggozdb.Maplemonk.BP_Sales_Salary ss left join (select area, case when beat_number_operations=0 then 9999 else beat_number_operations end as beat, last_day(date) as month, sum(net_sales) as net_sales, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return, sum(eggs_replaced) as eggs_replaced from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku where beat_number_operations is not null group by area, beat_number_operations, last_day(date))a on a.area = ss.classification and a.beat=ss.beat and a.month = last_day(to_date(concat(ss.year,\'-\',ss.month,\'-\',01),\'yyyy-mmmm-dd\')) group by ss.year,ss.month,ss.classification,ss.beat; create or replace table eggozdb.Maplemonk.ops_salary_new as select last_day(to_date(concat(os.year,\'-\',os.month,\'-\',01),\'yyyy-mmmm-dd\')) as month, os.classification as area, div0(sum(ifnull(replace(os.\"Total cost to be split into (per egg cost)\",\',\',\'\')::float,0)),sum(ifnull(eggs_sold,0))+sum(ifnull(eggs_replaced,0))) as ops_salary_total from eggozdb.Maplemonk.BP_Ops_Salary os left join (select area, last_day(date) as month, sum(net_sales) as net_sales, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return, sum(eggs_replaced) as eggs_replaced from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku where beat_number_operations is not null group by area, last_day(date))a on a.month = last_day(to_date(concat(os.year,\'-\',os.month,\'-\',01),\'yyyy-mmmm-dd\')) and a.area=os.classification group by os.year,os.month,os.classification; create or replace table eggozdb.Maplemonk.Unbranded_loss as select m.date, e.area, e.beat_new, sum((m.cost_per_egg - \"Selling Price/Egg\")*UB_eggs_sold) as UB_Loss_total_expense from ( select p.date, ub.type_new, ub.\"Selling Price/Egg\", p.cost_per_egg, ub.region from (select distinct to_date(month,\'yyyy/mm/dd\') date, case when type=\'Nutra\' then \'Nutra Plus\' else type end as type_new, region, \"Selling Price/Egg\" from eggozdb.maplemonk.BP_UB_SellingPrice) ub left join( select date_from_parts(year,month,day) as date, case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end as type, div0(sum(replace(amount,\',\',\'\')::float),sum(replace(eggs,\',\',\'\')::float)) as cost_per_egg from eggozdb.Maplemonk.procurement_cost_procure where eggs not in (\'bill total is 78524\',\'GRN missed\',\'2 tray extra\',\'2 tray short\') group by date_from_parts(year,month,day), case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end) p on date_trunc(month, p.date) = ub.date and p.type = ub.type_new)m left join (select distinct date, area, beat_new, type, ub_eggs_sold from eggozdb.Maplemonk.eggs_sold_new) e on m.date = e.date and m.type_new = e.type and m.region = left(e.area,len(e.area)-3) group by m.date, e.area, e.beat_new order by m.date desc, e.area desc,e.beat_new desc ; create or replace table eggozdb.maplemonk.packaging_manpower as select month, l.area as area, manpower_cost_per_egg from (select last_day(to_date(p.month)) as month, Region as area, --k.area as area, \"Net Gurgaon EPC Salary\"::float, sum( eggs_sold), div0(\"Net Gurgaon EPC Salary\"::float,sum(ifnull(eggs_sold,0))+sum(ifnull(eggs_replaced,0))) as manpower_cost_per_egg from eggozdb.Maplemonk.BP_Pkg_Manpower p left join (select area, last_day(date) as month, sum(eggs_replaced) as eggs_replaced, sum(eggs_sold) as eggs_sold from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku where beat_number_operations is not null and area in (\'Delhi-GT\', \'Gurgaon-GT\', \'Noida-GT\', \'NCR-MT\') group by area, last_day(date)) k on (case when k.area in (\'Delhi-GT\', \'Gurgaon-GT\', \'Noida-GT\', \'NCR-MT\') then \'NCR\' end)= p.region and k.month = last_day(to_date(p.month)) group by region, last_day(to_date(p.month)), \"Net Gurgaon EPC Salary\"::float) s left join (select distinct area from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku where area in (\'Delhi-GT\', \'Gurgaon-GT\', \'Noida-GT\', \'NCR-MT\')) l on s.area = (case when l.area in (\'Delhi-GT\', \'Gurgaon-GT\', \'Noida-GT\', \'NCR-MT\') then \'NCR\' end) ; create or replace table eggozdb.maplemonk.promo_loss as select e.area, p.date, e.beat_new beat, sum(e.promo_eggs_sold*p.cost_per_egg) as promo_expense from (select date_from_parts(year,month,day) as date, case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end as type_new, sum(replace(amount,\',\',\'\')::float) as amount, sum(replace(eggs,\',\',\'\')::float) as eggs, div0(sum(replace(amount,\',\',\'\')::float),sum(replace(eggs,\',\',\'\')::float)) as cost_per_egg from eggozdb.Maplemonk.procurement_cost_procure where eggs not in (\'bill total is 78524\',\'GRN missed\',\'2 tray extra\',\'2 tray short\') group by date_from_parts(year,month,day), case when type in (\'White\',\'white\') then \'White\' when type in (\'Brown\',\'brown\') then \'Brown\' when type in (\'Nutra Plus\',\'Nutra+\') then \'Nutra Plus\' else type end) p left join (select distinct date, area, beat_new, type, promo_eggs_sold from eggozdb.Maplemonk.eggs_sold_new) e on p.date = e.date and p.type_new = e.type group by p.date, e.area, e.beat_new order by p.date desc ; Create or replace table eggozdb.maplemonk.Date_area_retailer_dim as select cast(Date as date) Date, area_classification, code retailer_name, beat_number beat_number_original from eggozdb.maplemonk.date_dim cross join ( select distinct area_classification, code, beat_number, onboarding_date, onboarding_status from eggozdb.maplemonk.my_sql_retailer_retailer ) where date between \'2021-07-01\' and getdate() ; create or replace table eggozdb.maplemonk.temp1 as select a.date, a.area_classification area, a.retailer_name,b.beat_number_original, onboarding_date, onboarding_status, case when b.net_sales > 0 then 1 else 0 end as sales_visit_flag, case when b.date is not null then 1 else 0 end as all_visit_flag from eggozdb.maplemonk.Date_area_retailer_dim a left join eggozdb.maplemonk.summary_reporting_table_beat_retailer b on a.date=b.date and a.area_classification=b.area and a.retailer_name = b.retailer_name and a.beat_number_original = b.beat_number_original ; create or replace table eggozdb.maplemonk.Retailer_onboarded_as_of_date as select m.date , m.area , m.beat_number_original , sales_visits , all_visits , onboarded_as_of from (select date, area, beat_number_original, sum(sales_visit_flag) sales_visits, sum(all_visit_flag) all_visits from eggozdb.maplemonk.temp1 group by 1,2,3) m left join ( select a.date , a.area , a.beat_number_original , count(distinct case when b.onboarding_status = \'Onboarded\' and b.onboarding_Date <= a.date then retailer_name end) onboarded_as_of from ( select distinct date, area, beat_number_original from eggozdb.maplemonk.temp1 ) a left join ( select distinct date, area, retailer_name, onboarding_status,onboarding_Date, beat_number_original from eggozdb.maplemonk.temp1 ) b on a.date<=b.date and a.area=b.area and a.beat_number_original=b.beat_number_original group by 1,2,3 ) p on p.date = m.date and p.area = m.area and p.beat_number_original=m.beat_number_original ; create or replace table eggozdb.Maplemonk.rent_cost as select last_day(to_date(concat(os.year,\'-\',os.month,\'-\',01),\'yyyy-mmmm-dd\')) as month, os.classification as area, div0(sum(ifnull(replace(os.Rent,\',\',\'\')::float,0)),sum(ifnull(eggs_sold,0))+sum(ifnull(eggs_replaced,0))) as rent_cost_total from eggozdb.Maplemonk.BP_Rent os left join (select area, last_day(date) as month, sum(net_sales) as net_sales, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return, sum(eggs_replaced) as eggs_replaced from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku where beat_number_operations is not null group by area, last_day(date))a on a.month = last_day(to_date(concat(os.year,\'-\',os.month,\'-\',01),\'yyyy-mmmm-dd\')) and a.area=os.classification group by os.year,os.month,os.classification; create or replace table eggozdb.Maplemonk.profit as select a.area, a.date, a.beat, a.net_sales, a.amount_return, a.eggs_sold, a.eggs_return, a.eggs_replaced, a.eggs_promo, proc.Procurement_total_expense, fm.total_eggs_procured, fm.fm_total_expense, mm.mm_total_expense, lm.lm_total_expense, pack.packaging_total_expense as Packaging_total_expense_per_day, pack.packaging_total_expense_replaced as Packaging_total_expense_replaced_per_day, pack.packaging_total_expense_return as Packaging_total_expense_return_per_day, pack.packaging_total_expense_promo as Packaging_total_expense_promo_per_day, disc.Discount as discount_total, m.manpower_cost_per_egg*(ifnull(a.eggs_sold,0) + ifnull(a.eggs_replaced,0)) as Manpower_total_expense, ss.sales_salary_total/right(last_day(a.date),2) as Sales_Salary_total_per_day, os.ops_salary_total*(ifnull(a.eggs_sold,0) + ifnull(a.eggs_replaced,0)) as Ops_Salary_total_per_day, rc.RENT_COST_TOTAL*(ifnull(a.eggs_sold,0) + ifnull(a.eggs_replaced,0)) as Rent_total_expense, case when a.area = \'NCR-MT\' then a.net_Sales*0.15 else c.credit_loss_total end as credit_loss_total, ub.ub_loss_total_expense, pl.promo_expense, aod.sales_visits, aod.all_visits, aod.onboarded_as_of from ( select area, date, case when beat_number_operations=0 then 9999 else beat_number_operations end as beat, sum(net_sales) as net_sales, sum(amount_return) as amount_return, sum(eggs_sold) as eggs_sold, sum(eggs_return) as eggs_return, sum(eggs_replaced) as eggs_replaced, sum(eggs_promo) as eggs_promo from eggozdb.Maplemonk.summary_reporting_table_beat_retailer_sku where beat is not null group by area,date ,case when beat_number_operations=0 then 9999 else beat_number_operations end )a left join (select distinct date,area,beat_new,Procurement_total_expense from eggozdb.Maplemonk.procurement_new) proc on a.date=proc.date and a.beat=proc.beat_new and a.area=proc.area left join (select distinct date_new,area,beat,fm_total_expense,total_eggs_procured from eggozdb.Maplemonk.first_mile_new) fm on a.date=fm.date_new and a.beat=fm.beat and a.area=fm.area left join (select distinct date_new,area,beat_new,mm_total_expense from eggozdb.Maplemonk.mid_mile_new) mm on a.date=mm.date_new and a.beat=mm.beat_new and a.area=mm.area left join (select distinct date,beat,area,lm_total_expense from eggozdb.Maplemonk.last_mile_new) lm on a.date=lm.date and a.beat=lm.beat and a.area=lm.area left join (select distinct month,date, beat,region,packaging_total_expense,packaging_total_expense_replaced,packaging_total_expense_return,packaging_total_expense_promo from eggozdb.Maplemonk.packaging_new) pack on a.date=pack.date and a.beat=pack.beat and left(a.area,charindex(\'-\',a.area)-1)=pack.region left join (select cast(timestampadd(minute,660,o.date) as date) as Date, rr.area_classification as area, ba.beat_number as beat_number_operations, sum(o.scheme_discount_amount) as Discount from eggozdb.Maplemonk.my_sql_order_order o left join eggozdb.Maplemonk.my_sql_retailer_retailer rr ON o.retailer_id =rr.id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment ba on ba.id = o.beat_assignment_id group by cast(timestampadd(minute,660,o.date) as date), rr.area_classification, ba.beat_number) disc on a.date=disc.date and a.beat=disc.beat_number_operations and a.area=disc.area left join eggozdb.maplemonk.packaging_manpower m on last_day(a.date)=m.month and a.area=m.area left join eggozdb.Maplemonk.sales_salary_new ss on last_day(a.date)=ss.month and a.area=ss.area and a.beat=ss.beat left join eggozdb.Maplemonk.ops_salary_new os on last_day(a.date)=os.month and a.area=os.area left join eggozdb.maplemonk.credit_loss c on a.date=c.date and a.area=c.area and a.beat=c.beat_number_operations left join eggozdb.Maplemonk.Unbranded_loss ub on a.date=ub.date and a.area=ub.area and a.beat = ub.beat_new left join eggozdb.maplemonk.promo_loss pl on a.date=pl.date and a.area=pl.area and a.beat=pl.beat left join eggozdb.Maplemonk.rent_cost rc on last_day(a.date)=rc.month and a.area=rc.area left join eggozdb.maplemonk.Retailer_onboarded_as_of_date aod on aod.date = a.date and aod.area = a.area and aod.beat_number_original = a.beat",
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
                        