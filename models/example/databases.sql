{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.affiliate_snitch as WITH fact_revenue AS ( SELECT upper(discount_code) as discount_code, order_id, order_name, tags, order_timestamp::Date order_date, upper(final_utm_source) as final_utm_source, customer_flag, line_item_id, phone, email, final_utm_medium, final_utm_campaign, SUM(gross_sales) AS total_sales, SUM(discount) AS total_discount, sum(case when lower(ordeR_status) = \'cancelled\' then gross_sales end) cancelled_sales FROM SNITCH_DB.maplemonk.fact_items_snitch GROUP BY 1, 2, 3, 4, 5, 6,7,8,9,10,11,12 ORDER BY 2,6 ), affiliates AS ( SELECT distinct upper(source) as source, upper(partner) as partner, upper(discount_code) as discount_code FROM snitch_db.maplemonk.s3_affiliate_code_mapping GROUP BY 1,2,3 union all select distinct \'ZOMATO\' as source, \'ZOMATO\' as partner, upper(code) as discount_code from snitch_db.maplemonk.zomato_discount_codes ), fact_returns AS ( SELECT order_date, order_id, SPLIT_PART(saleorderitemcode, \'-\', 1) AS saleorderitemcode, SUM(CASE WHEN return_quantity <> 0 AND cancelled_quantity = 0 THEN selling_price END) AS return_sales, SUM(CASE WHEN cancelled_quantity <> 0 THEN selling_price END) AS cancelled_sales FROM SNITCH_DB.MAPLEMONK.unicommerce_fact_items_snitch GROUP BY 1, 2, 3 ), fact_monthly_target AS ( SELECT date, partner as src, REPLACE(\"Monthly Cost\", \',\', \'\')::float AS monthly_cost, REPLACE(monthly_target, \',\', \'\')::float AS monthly_target FROM snitch_db.maplemonk.affiliates_target ), fact_merge AS ( SELECT DISTINCT upper(ma.source) as source, upper(fis.final_utm_source) as final_utm_source, 1 AS utm_flag, upper(ma.partner) as partner FROM snitch_db.maplemonk.s3_affiliate_code_mapping ma LEFT JOIN snitch_db.maplemonk.fact_items_snitch fis ON LOWER(ma.source) = LOWER(fis.final_utm_source) GROUP BY 1,2,3,4 ), discount_code_merge AS ( SELECT coalesce(frt.order_date, fr.ordeR_date) ordeR_date, upper(af.discount_code) as discount_code, fr.order_name, fr.tags, upper(trim(af.partner)) as partner_af, upper(trim(af.source)) as source_af, fr.phone, fr.email, fr.customer_flag, case when upper(trim(af.partner)) = \'VCOMMISSION\' and frt.order_date::date > \'2023-12-19\' then fr.final_utm_campaign when upper(trim(af.partner)) = \'CLICKONIK\' and frt.order_date::date >= \'2023-12-31\' then fr.final_utm_campaign when upper(trim(af.partner)) = \'ICUBEWIRE\' and frt.order_date::date >= \'2024-01-04\' then fr.final_utm_campaign when upper(trim(af.partner)) = \'SWOPSTORE\' and frt.order_date::date >= \'2023-12-25\' then fr.final_utm_campaign when upper(trim(af.partner)) = \'ADMITAD\' and upper(fr.final_utm_medium) like \'%AFFILIATE%\' then fr.final_utm_campaign when upper(trim(af.partner)) = \'ADSFLOURISH\' and upper(fr.final_utm_medium) like \'%AFFILIATE%\' then fr.final_utm_campaign when upper(trim(af.partner)) = \'ADCANAPOUS\' and upper(fr.final_utm_medium) like \'%AFFILIATE%\' then fr.final_utm_campaign when upper(trim(af.partner)) = \'PRUDENTADS\' and upper(fr.final_utm_medium) like \'%AFFILIATE%\' then fr.final_utm_campaign else fr.final_utm_medium end final_utm_medium, CASE WHEN lower(partner_af) = \'grabon\' then 80 WHEN lower(partner_af) = \'admitad\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'admitad\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'pinchpennies\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'kickcash\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'adcanapous\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'cred\' and lower(customer_flag) = \'new\' then 200 WHEN lower(partner_af) = \'cred\' and lower(customer_flag) = \'repeated\' then 100 WHEN lower(partner_af) = \'wishlink\'and lower(customer_flag) = \'new\' then 0.18*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'wishlink\'and lower(customer_flag) = \'repeated\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'unstop\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'icubewire\' and coalesce(frt.order_date, fr.ordeR_date) between \'2023-11-15\' and \'2023-12-15\' then 0.09*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'icubewire\' and coalesce(frt.order_date, fr.ordeR_date) between \'2024-02-01\' and \'2024-03-31\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'icubewire\' and coalesce(frt.order_date, fr.ordeR_date) > \'2024-04-01\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'icubewire\' and coalesce(frt.order_date, fr.ordeR_date) > \'2024-04-01\' and lower(customer_flag) = \'repeated\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'icubewire\' then 0.07*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'jio\' then 200 WHEN lower(partner_af) = \'clickonik\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'clickonik\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'mydala\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'mydala\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'vcommission\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'vcommission\'and lower(customer_flag) = \'repeated\' then 0.06*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'adsflourish\'and lower(customer_flag) = \'new\' then 0.07*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'adsflourish\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'twid\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'twid\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'mobilog\' then 0.06*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'rohit\' and coalesce(frt.order_date, fr.ordeR_date) > \'2024-04-01\' and lower(customer_flag) = \'new\' then 0.06*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'rohit\' and coalesce(frt.order_date, fr.ordeR_date) > \'2024-04-01\' and lower(customer_flag) = \'repeated\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'rohit\' then 0.07*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'swopstore\' and lower(customer_flag) = \'new\' then 0.07*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'swopstore\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float end cost, COALESCE(SUM(fr.total_sales), 0) AS sales, COALESCE(SUM(fr.total_discount), 0) AS discount, COALESCE(SUM(case when fr.cancelled_sales is null then frt.return_sales end), 0) AS returns, COALESCE(SUM(frt.cancelled_sales),sum(fr.cancelled_sales), 0) AS cancelled, NULL AS utm_flag, \'discount_code\' AS source_type FROM affiliates af LEFT JOIN fact_revenue fr ON LOWER(af.discount_code) = LOWER(fr.discount_code) LEFT JOIN fact_returns frt ON fr.order_id = frt.order_id AND fr.line_item_id = SPLIT_PART(frt.saleorderitemcode, \'-\', 1) WHERE af.discount_code IS NOT NULL GROUP BY 1, 2, 3, 4, 5,6,7,8,9,10 ), utm_source_merge AS ( SELECT coalesce(frt.order_date, fr.ordeR_date) ordeR_date, upper(fr.discount_code) as discount_code, fr.order_name, fr.tags, upper(trim(fr.final_utm_source)) as source_fr, upper(trim(fr.final_utm_source)) as partner_fr, fr.phone, fr.email, fr.customer_flag, case when upper(trim(fr.final_utm_source)) = \'VCOMMISSION\' and frt.order_date::date > \'2023-12-19\' then fr.final_utm_campaign when upper(trim(fr.final_utm_source)) = \'CLICKONIK\' and frt.order_date::date >= \'2023-12-31\' then fr.final_utm_campaign when upper(trim(fr.final_utm_source)) = \'ICUBEWIRE\' and frt.order_date::date >= \'2024-01-04\' then fr.final_utm_campaign when upper(trim(fr.final_utm_source)) = \'SWOPSTORE\' and frt.order_date::date >= \'2023-12-25\' then fr.final_utm_campaign when upper(trim(fr.final_utm_source)) = \'ADMITAD\' and upper(fr.final_utm_medium) like \'%AFFILIATE%\' then fr.final_utm_campaign when upper(trim(fr.final_utm_source)) = \'ADSFLOURISH\' and upper(fr.final_utm_medium) like \'%AFFILIATE%\' then fr.final_utm_campaign when upper(trim(fr.final_utm_source)) = \'ADCANAPOUS\' and upper(fr.final_utm_medium) like \'%AFFILIATE%\' then fr.final_utm_campaign when upper(trim(fr.final_utm_source)) = \'PRUDENTADS\' and upper(fr.final_utm_medium) like \'%AFFILIATE%\' then fr.final_utm_campaign else fr.final_utm_medium end final_utm_medium, CASE WHEN lower(source_fr) = \'grabon\' then 80 WHEN lower(source_fr) = \'admitad\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'admitad\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'pinchpennies\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'kickcash\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'adcanapous\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'cred\' and lower(customer_flag) = \'new\' then 200 WHEN lower(source_fr) = \'cred\' and lower(customer_flag) = \'repeated\' then 100 WHEN lower(source_fr) = \'wishlink\'and lower(customer_flag) = \'new\' then 0.18*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'wishlink\'and lower(customer_flag) = \'repeated\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'unstop\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'icubewire\' and coalesce(frt.order_date, fr.ordeR_date) between \'2023-11-15\' and \'2023-12-15\' then 0.09*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'icubewire\' and coalesce(frt.order_date, fr.ordeR_date) between \'2024-02-01\' and \'2024-03-31\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'icubewire\' and coalesce(frt.order_date, fr.ordeR_date) > \'2024-04-01\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'icubewire\' and coalesce(frt.order_date, fr.ordeR_date) > \'2024-04-01\' and lower(customer_flag) = \'repeated\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'icubewire\' then 0.07*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'jio\' then 200 WHEN lower(source_fr) = \'clickonik\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'clickonik\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'mydala\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'mydala\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'vcommission\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'vcommission\'and lower(customer_flag) = \'repeated\' then 0.06*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'adsflourish\'and lower(customer_flag) = \'new\' then 0.07*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'adsflourish\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'twid\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'twid\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'mobilog\' then 0.06*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'rohit\' and coalesce(frt.order_date, fr.ordeR_date) > \'2024-04-01\' and lower(customer_flag) = \'new\' then 0.06*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'rohit\' and coalesce(frt.order_date, fr.ordeR_date) > \'2024-04-01\' and lower(customer_flag) = \'repeated\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'rohit\' then 0.07*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'swopstore\' and lower(customer_flag) = \'new\' then 0.07*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'swopstore\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float end cost, COALESCE(SUM(fr.total_sales), 0) AS sales, COALESCE(SUM(fr.total_discount), 0) AS discount, COALESCE(SUM(case when fr.cancelled_sales is null then frt.return_sales end), 0) AS returns, COALESCE(SUM(frt.cancelled_sales),sum(fr.cancelled_sales), 0) AS cancelled, fm.utm_flag, \'utm_source\' AS source_type FROM fact_revenue fr LEFT JOIN fact_merge fm ON LOWER(fm.source) = lower(fr.final_utm_source) LEFT JOIN fact_returns frt ON fr.order_id = frt.order_id AND fr.line_item_id = SPLIT_PART(frt.saleorderitemcode, \'-\', 1) WHERE fm.utm_flag = 1 GROUP BY 1, 2, 3, 4, 5,6,7,8, 9,10,fm.utm_flag ), union_merge AS ( SELECT * FROM discount_code_merge UNION SELECT * FROM utm_source_merge ) SELECT * FROM union_merge um LEFT JOIN fact_monthly_target fmt on date_trunc(\'month\',um.order_date::Date) = date_trunc(\'month\',fmt.date::date) and lower(um.partner_af) = lower(fmt.src);",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from snitch_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        