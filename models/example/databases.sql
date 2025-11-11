{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.symphony_wh_funnel_metrics_overall as select cast(format_date(\'%Y-%m-%d\', parse_date(\'%Y%m%d\', gaf.date)) as date) as date, cast(newUsers as int64) NewUsers, cast(sessions as int64) sessions, cast(checkouts as int64) checkouts, cast(addToCarts as int64) addtocarts, cast(totalUsers as int64) totalUsers, cast(transactions as int64) transactions, cast(engagedSessions as int64) engagedSessions, cast(screenPageViews as int64) screenPageViews, s.total_sales, s.total_orders, s.total_tax, gap.product_views from `Maplemonk.Symphony_GA4_funnel_overall_funnel_by_date` gaf left join (select date, sum(total_sales) total_sales, sum(total_orders) total_orders, sum(total_tax) total_tax from maplemonk.symphony_wh_sales_cost_source where upper(marketplace) = \'WEBSITE IN\' group by 1 ) s on s.date = cast(format_date(\'%Y-%m-%d\', parse_date(\'%Y%m%d\', gaf.date)) as date) left join (select date, sum(itemsViewed) as product_views from maplemonk.ga4_products_funnel_metrics group by 1 ) gap on gap.date = gaf.date; create or replace table maplemonk.symphony_ga4_engagement_report as select cast(format_date(\'%Y-%m-%d\', parse_date(\'%Y%m%d\', g.date)) as date) as date, cast(averageSessionDuration as float64)/60 avg_session_time_in_minutes, cast(bounceRate as float64) Bounce_Rate, m.ctr, cast(sessions as int64) sessions, s.total_sales, s.total_orders, cast(transactions as int64) transactions from maplemonk.GA4_engagement_report g left join (select date, sum(total_sales) total_sales, sum(total_orders) total_orders, sum(total_tax) total_tax, sum(TOTAL_Unique_Customers) Total_Customers, sum(TOTAL_New_Customers) New_Customers, sum(Repeat_Customers) Repeat_Customers, from maplemonk.symphony_wh_sales_cost_source where upper(marketplace) = \'WEBSITE IN\' group by 1 ) s on s.date = cast(format_date(\'%Y-%m-%d\', parse_date(\'%Y%m%d\', g.date)) as date) left join (select date, avg(safe_divide(clicks,impressions)) ctr from maplemonk.symphony_wh_marketing_consolidated group by 1 ) m on m.date = cast(format_date(\'%Y-%m-%d\', parse_date(\'%Y%m%d\', g.date)) as date) ;",
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
            