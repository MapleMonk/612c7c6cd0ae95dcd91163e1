{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table eggozdb.maplemonk.Franchise_Sales_Tracker as select st.date,tt.dealer_name,st.code, avg(tt.target_revenue) target_revenue, tt.dsr_name,tt.so_name, sum(st.revenue) revenue, sum(st.dsr_sales) dsr_sales,sum(st.promoter_sales) promoter_sales,st.bill_count,st.tertiary_bill_count,st.uob, sum(st.eggs_sold) eggs_sold, sum(dsr_eggs_sold) dsr_eggs_sold, sum(promoter_eggs_sold) promoter_eggs_sold, Trays, CASE WHEN TARGET_REVENUE > 0 THEN ROUND(SUM(REVENUE) / AVG(TARGET_REVENUE) * 100, 2) ELSE \'0\' END as Sales_achivement, COUNT(DISTINCT st.date) OVER () AS total_days, DAY(MAX(st.date)) AS day_of_max_date from bi_franchise_sales_tracker_targets tt left join (select coalesce(ps.retailer_name,ts.dealer_name,crm.code) as code,dsr_name,so_name,sr_name, coalesce(ps.date,ts.delivery_date,crm.crm_date) date,ps.bill_count,ts.tertiary_bill_count,ts.uob, ps.revenue,ts.sku_sale as DSR_sales,crm.crm_sale as Promoter_Sales, ps.eggs_sold,ts.eggs_sold DSR_eggs_Sold,crm.eggs_sold as Promoter_eggs_sold, (ps.eggs_sold)/25 as Trays from (select retailer_name,date,sum(revenue) revenue,sum(eggs_sold) eggs_sold ,so.dsr_name,so.so_name,so.sr_name, count(distinct(concat(retailer_name,date))) bill_count from eggozdb.maplemonk.primary_and_secondary_sku ps left join eggozdb.maplemonk.beat_tagging so on ps.retailer_name=so.code and ps.area_classification = so.area_classification group by retailer_name,date,so.dsr_name,so.so_name,so.sr_name ) ps full outer join (select dealer_name, delivery_date,sum(sku_sale) sku_sale,sum(eggs_sold) eggs_sold, count(distinct(concat(dealer_name,delivery_date))) tertiary_bill_count, count(distinct(concat(dealer_name,code))) uob from tertiary_sales group by dealer_name,delivery_date ) ts on ps.retailer_name=ts.dealer_name and ps.date=ts.delivery_date full outer join (select code,crm_date,sum(crm_sale)crm_sale,sum(crm_eggs_sold) eggs_sold from crm_overview group by code,crm_date ) crm on ts.dealer_name = crm.code and ts.delivery_date = crm.crm_date ) st on tt.Dealer_Code=SPLIT_PART(st.code, \'*\', 1) and tt.month = month(st.date) and tt.year = year(st.date) group by tt.dealer_name,st.code,tt.dsr_name,tt.so_name, st.date,Trays,st.bill_count,st.tertiary_bill_count,st.uob,tt.TARGET_REVENUE ;",
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
            