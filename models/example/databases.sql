{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table prd_db.justherbs.dwh_pandlsummary as select x.*, div0(coalesce(y.spend, z.spend,m.spend, n.spend, o.spend, p.spend, q.spend, r.spend), count(1) over (partition by x.order_timestamp::date, x.channel order by 1)) spend from( select m.* ,CASE WHEN MONTH(m.order_timestamp::date) >= 4 THEN YEAR(m.order_timestamp::date) + 1 ELSE YEAR(m.order_timestamp::date) END AS financial_year ,d.return_sales ,e.customer_id_final ,case when lower(final_utm_channel) in (\'google\',\'meta\',\'crm\',\'organic\',\'affiliates (cps)\',\'influencer marketing\',\'partnerships & alliances (cpe)\',\'native ads\',\'influencer marketing\',\'others\') then upper(final_utm_channel) else \'ALL_OTHERS\' end channel from (select a.* ,b.final_utm_source ,b.final_utm_channel ,b.shopify_new_customer_flag ,b.tax from prd_db.justherbs.DWH_GROSS_CONTRIBUTION_JH a left join ( select order_name, final_utm_source, final_utm_channel, shopify_new_customer_flag, sum(tax) tax from prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS group by 1,2,3,4 ) b on a.ordeR_name = b.ordeR_name )m left join ( select reference_code, sum(total_return_amount) return_sales from prd_db.justherbs.dwh_easyecom_returns_fact_items group by 1 ) d on m.ordeR_name = d.reference_code left join ( select distinct reference_code, customer_id_final from prd_db.justherbs.dwh_sales_consolidated ) e on m.ordeR_name = e.reference_code ) x left join (select date ,replace(google,\',\',\'\')::float spend from datalake_db.justherbs.trn_spends_jh) y on x.ordeR_timestamp::date = y.date and x.channel = \'GOOGLE\' left join (select date ,replace(meta,\',\',\'\')::float spend from datalake_db.justherbs.trn_spends_jh) z on x.ordeR_timestamp::date = z.date and x.channel = \'META\' left join (select date ,replace(\"Affiliates (CPS)\",\',\',\'\') spend from datalake_db.justherbs.trn_spends_jh) m on x.ordeR_timestamp::date = m.date and x.channel = \'AFFILIATES (CPS)\' left join (select date ,replace(CRM,\',\',\'\') spend from datalake_db.justherbs.trn_spends_jh) n on x.ordeR_timestamp::date = n.date and x.channel = \'CRM\' left join (select date ,replace(\"Partnerships & Alliances (CPE)\",\',\',\'\') spend from datalake_db.justherbs.trn_spends_jh) o on x.ordeR_timestamp::date = n.date and x.channel = \'PARTNERSHIPS & ALLIANCES (CPE)\' left join (select date ,replace(\"Native Ads\",\',\',\'\') spend from datalake_db.justherbs.trn_spends_jh) p on x.ordeR_timestamp::date = n.date and x.channel = \'NATIVE ADS\' left join (select date ,replace(OTHERS,\',\',\'\') spend from datalake_db.justherbs.trn_spends_jh) q on x.ordeR_timestamp::date = n.date and x.channel = \'OTHERS\' left join (select date ,replace(\"Influencer Marketing\",\',\',\'\') spend from datalake_db.justherbs.trn_spends_jh) r on x.ordeR_timestamp::date = n.date and x.channel = \'INFLUENCER MARKETING\' ; create or replace table prd_db.justherbs.dwh_cltv as select a.ordeR_month ,div0(sum(total_sales), count(distinct customer_id_final)) aov ,div0(sum(spend), count(distinct customer_id_final)) cac ,div0(count(distinct ordeR_name), count(distinct customer_id_final)) apf ,div0(sum(ifnull(gc, 0)), sum(ifnull(nr, 0))) gc_percent , div0(AOV*div0(sum(ifnull(gc, 0)), sum(ifnull(nr, 0)))*apf,cac) cltv_cac_ratio from ( select distinct date_trunc(\'month\',order_timestamp::Date) order_month from prd_db.justherbs.dwh_gross_contribution_JH ) a left join prd_db.justherbs.dwh_pandlsummary b on datediff(month,date_trunc(\'month\',b.ordeR_timestamp::Date),order_month) between 1 and 12 group by 1 order by 1 desc ; create or replace table prd_db.justherbs.dwh_cltv_channel as select a.ordeR_month ,b.channel ,div0(sum(total_sales), count(distinct customer_id_final)) aov ,div0(sum(spend), count(distinct customer_id_final)) cac ,div0(count(distinct ordeR_name), count(distinct customer_id_final)) apf ,div0(sum(ifnull(gc, 0)), sum(ifnull(nr, 0))) gc_percent , div0(AOV*div0(sum(ifnull(gc, 0)), sum(ifnull(nr, 0)))*apf,cac) cltv_cac_ratio from ( select distinct date_trunc(\'month\',order_timestamp::Date) order_month from prd_db.justherbs.dwh_gross_contribution_jh ) a left join prd_db.justherbs.dwh_pandlsummary b on datediff(month,date_trunc(\'month\',b.ordeR_timestamp::Date),order_month) between 1 and 12 group by 1,2 order by 1,2 desc ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from PRD_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            