{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.retailer_ranking as select t2.code, t2.onboarding_status, t2.distributor_id, t2.category_id, t1.*, sum(\"Revenue\") over (partition by year(date_),month(date_),\"Area\" order by \"Revenue\" desc range between unbounded preceding and current row)/(sum(\"Revenue\") over (partition by year(date_),month(date_),\"Area\" order by month(date_))+1) as cumulative_revenue_contribution, (retailer_count-\"Rank\"::int)/retailer_count ranking_average from (select *, count(\"Retailer_name\") over (partition by year(date_),month(date_),\"Area\" order by month(date_)) retailer_count, 65*1/(1+exp(-1*log(10,\"Revenue\"+1))) - 5*1/(1+exp(-1*(case when no_of_replacements = 0 then 0 else no_of_replacements end)/(case when no_of_bills = 0 then 1 else no_of_bills end))) - 15*1/(1+exp(-1*(\"eggs_replaced\"/(\"Eggs Sold\"+1)))) - 5*1/(1+exp(-1*(\"eggs_return\"/(\"Eggs Sold\"+1)))) + 10*1/(1+exp(-1*\"landing_price\")) + 5*1/(1+exp(-1*log(10,(\"Eggs Sold\"-(\"Eggs Sold\"/(case when no_of_bills = 0 then 1 else no_of_bills end)))+1))) + 50*revenue_contribution as score, rank() over (partition by date_, \"Area\" order by score desc) as \"Rank\" from (select ps.*, case when ps1.no_of_bills is null then 0 else ps1.no_of_bills end as no_of_bills, case when ps2.no_of_replacements is null then 0 else ps2.no_of_replacements end as no_of_replacements from ( SELECT distinct retailer_name AS \"Retailer_name\", area_classification as \"Area\", date_from_parts(year(date), month(date), 01) date_, sum(revenue) over (partition by year(date),month(date),retailer_name,retailer_id,area_classification order by month(date))/(sum(revenue) over (partition by year(date),month(date),area_classification order by month(date))+1) as revenue_contribution, sum(revenue) over (partition by year(date),month(date),retailer_name,retailer_id,area_classification order by month(date)) AS \"Revenue\", sum(eggs_sold) over (partition by year(date),month(date),retailer_name,retailer_id,area_classification order by month(date)) AS \"Eggs Sold\", sum(revenue) over (partition by year(date),month(date),retailer_name,retailer_id,area_classification order by month(date))/(sum(eggs_sold) over (partition by year(date),month(date),retailer_name,retailer_id,area_classification order by month(date))+1) as \"landing_price\", sum(eggs_return) over (partition by year(date),month(date),retailer_name,retailer_id,area_classification order by month(date)) as \"eggs_return\", sum(amount_return) over (partition by year(date),month(date),retailer_name,retailer_id,area_classification order by month(date)) as \"amount_return\", sum(eggs_replaced) over (partition by year(date),month(date),retailer_name,retailer_id,area_classification order by month(date)) as \"eggs_replaced\", retailer_id FROM eggozdb.maplemonk.primary_and_secondary where revenue is not null and category_id <> 3 ) ps left join ( select distinct retailer_name, date_from_parts(year(date), month(date), 01) date_, count(date) over (partition by year(date),month(date),retailer_name order by month(date)) no_of_bills from eggozdb.maplemonk.primary_and_secondary where eggs_sold is not null and eggs_sold > 0 and category_id <> 3 ) ps1 on ps1.retailer_name = ps.\"Retailer_name\" and ps.date_ = ps1.date_ left join ( select distinct retailer_name, date_from_parts(year(date), month(date), 01) date_, count(date) over (partition by year(date),month(date),retailer_name order by month(date)) no_of_replacements from eggozdb.maplemonk.primary_and_secondary where eggs_replaced is not null and eggs_replaced > 0 and category_id <> 3 ) ps2 on ps2.retailer_name = ps.\"Retailer_name\" and ps.date_ = ps2.date_ ) group by \"Retailer_name\",\"Area\",date_,\"Revenue\",\"Eggs Sold\",\"landing_price\",\"eggs_return\",\"eggs_replaced\",retailer_id, no_of_bills, no_of_replacements, revenue_contribution ) t1 right join (select code, id, area_classification, onboarding_status, distributor_id, category_id from eggozdb.maplemonk.my_sql_retailer_retailer where category_id<>3) t2 on t1.retailer_id = t2.id ;",
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
                        