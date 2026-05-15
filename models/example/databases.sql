{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table neon-poetry-482906-j7.Maplemonk.atovio_pandl as with orders as ( select cast(order_Date as date) order_Date, reference_code, sku, marketplace, mrp_sales, total_cogs, CASE WHEN LOWER(return_type) = \'return\' THEN COALESCE(total_cogs, 0)ELSE 0 END AS Return_cogs, tax, selling_price selling_price, mrp_sales - selling_price as discount, order_status, return_type, total_return_amount, product_category, product_sub_category from neon-poetry-482906-j7.Maplemonk.neon_poetry_482906_j7_sales_consolidated ), marketing_spend as ( select date, sum(spend) marketing_spend from neon-poetry-482906-j7.Maplemonk.neon_poetry_482906_j7_MARKETING_CONSOLIDATED group by 1 ), influencer_spend as ( select date_trunc(cast(date as date), month) month, sum(cast(replace(Actual_Cost____,\',\',\'\') as float64)) influencer_spend from maplemonk.gs_influencer group by 1 ), agency_spend as ( select date_trunc(cast(month as date), month) month, sum(cast(replace(Total_Price__INR_,\',\',\'\') as float64)) agency_spend from maplemonk.gs_agency group by 1 ), content_spend as ( select date_trunc(cast(month as date), month) month, sum(cast(replace(Original_Amount,\',\',\'\') as float64)) content_spend from maplemonk.gs_content_creative group by 1 ), software_spend_month as ( select date_trunc(cast(month as date), month) month, sum(cast(replace(Price__INR___excl_GST_,\',\',\'\') as float64)) software_spend from `Maplemonk.Atovio_db_gs_Software_Tools` where plan_type = \'Monthly\' group by 1 ) select o.*, safe_divide(marketing_spend,count(*) over (partition by o.order_Date, o.return_type order by 1)) marketing_spend, safe_divide(influencer_spend, count(*) over (partition by date_trunc(o.ordeR_date, month), o.return_type order by 1)) influencer_Spend, safe_divide(agency_Spend, count(*) over (partition by date_trunc(o.ordeR_date, month), o.return_type order by 1)) agency_Spend, safe_divide(content_spend, count(*) over (partition by date_trunc(o.ordeR_date, month), o.return_type order by 1)) content_spend, safe_divide(software_spend, count(*) over (partition by date_trunc(o.ordeR_date, month), o.return_type order by 1)) software_spend, from orders o left join marketing_spend s on o.order_Date = s.date and o.return_type is null left join influencer_spend ins on date_trunc(o.ordeR_date, month) = ins.month and o.return_type is null left join agency_spend ags on date_trunc(o.ordeR_date, month) = ags.month and o.return_type is null left join content_spend cs on date_trunc(o.ordeR_date, month) = cs.month and o.return_type is null left join software_spend_month spm on date_trunc(o.ordeR_date, month) = spm.month and o.return_type is null;",
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
            