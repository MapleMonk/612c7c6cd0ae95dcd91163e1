{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE or replace TABLE snitch_db.maplemonk.partner_monthly_spends ( month DATE, partner VARCHAR(50), cost FLOAT ); INSERT INTO snitch_db.maplemonk.partner_monthly_spends (month, partner, cost) VALUES (\'2023-12-01\', \'Zomato\', 624977), (\'2024-01-01\', \'Zomato\', 594114), (\'2024-02-01\', \'Zomato\', 799388), (\'2024-07-01\', \'fk\', 79526.7), (\'2024-07-01\', \'cred\', 291939), (\'2024-06-01\', \'cred\', 356890), (\'2024-05-01\', \'cred\', 311752) ; create or replace table snitch_db.maplemonk.utm_discount_affiliate_comparison as select order_name, order_date,max(sales) sales,max(case when source_type = \'utm_source\' then partner_af end) utm_partner, min(case when source_type =\'discount_code\' then partner_af end) discount_partner from snitch_db.maplemonk.affiliate_snitch_intermediate group by 1,2 ; create or replace table snitch_db.maplemonk.affiliate_snitch_intermediate as WITH fact_revenue AS ( SELECT upper(discount_code) as discount_code, order_id, order_name, tags, order_timestamp::Date order_date, upper(final_utm_source) as final_utm_source, customer_flag, line_item_id, phone, email, final_utm_medium, final_utm_campaign, case when webshopney = \'Web\' then \'Web\' else \'App\' end web_app_flag, SUM(gross_sales) AS total_sales, SUM(discount) AS total_discount, sum(case when lower(ordeR_status) = \'cancelled\' then gross_sales end) cancelled_sales, sum(dto_sales) as dto_sales, sum(rto_sales) as rto_sales FROM SNITCH_DB.maplemonk.fact_items_snitch where upper(left(ifnull(discount_code,\'a\'),5)) <> \'INFLU\' GROUP BY 1, 2, 3, 4, 5, 6,7,8,9,10,11,12,13 ORDER BY 2,6 ), affiliates AS ( SELECT distinct upper(source) as source, upper(partner) as partner, upper(discount_code) as discount_code FROM snitch_db.maplemonk.s3_affiliate_code_mapping GROUP BY 1,2,3 union all select distinct \'ZOMATO\' as source, \'ZOMATO\' as partner, upper(code) as discount_code from snitch_db.maplemonk.zomato_discount_codes ), fact_returns AS ( SELECT order_date, order_id, SPLIT_PART(saleorderitemcode, \'-\', 1) AS saleorderitemcode, SUM(CASE WHEN return_quantity <> 0 AND cancelled_quantity = 0 THEN selling_price END) AS return_sales, SUM(CASE WHEN cancelled_quantity <> 0 THEN selling_price END) AS cancelled_sales FROM SNITCH_DB.MAPLEMONK.unicommerce_fact_items_snitch GROUP BY 1, 2, 3 ), fact_monthly_target AS ( SELECT date, partner as src, REPLACE(\"Monthly Cost\", \',\', \'\')::float AS monthly_cost, REPLACE(monthly_target, \',\', \'\')::float AS monthly_target FROM snitch_db.maplemonk.affiliates_target ), fact_merge AS ( SELECT DISTINCT upper(ma.source) as source, upper(fis.final_utm_source) as final_utm_source, 1 AS utm_flag, upper(trim(ma.partner)) as partner FROM snitch_db.maplemonk.s3_affiliate_code_mapping ma LEFT JOIN (select * from snitch_db.maplemonk.fact_items_snitch where upper(left(ifnull(discount_code,\'a\'),5)) <> \'INFLU\' ) fis ON LOWER(ma.source) = LOWER(fis.final_utm_source) GROUP BY 1,2,3,4 ), discount_code_merge AS ( SELECT coalesce(frt.order_date, fr.ordeR_date) ordeR_date, upper(af.discount_code) as discount_code, fr.order_name, fr.tags, upper(trim(af.partner)) as partner_af, upper(trim(af.source)) as source_af, fr.phone, fr.email, fr.customer_flag, fr.web_app_flag, case when partner_af = \'VCOMMISSION\' and frt.order_date::date > \'2023-12-19\' then fr.final_utm_campaign when partner_af = \'CLICKONIK\' and frt.order_date::date >= \'2023-12-31\' then fr.final_utm_campaign when partner_af = \'ICUBEWIRE\' and frt.order_date::date >= \'2024-01-04\' then fr.final_utm_campaign when partner_af = \'SWOPSTORE\' and frt.order_date::date >= \'2023-12-25\' then fr.final_utm_campaign when partner_af not in (\'VCOMMISSION\',\'CLICKONIK\',\'ICUBEWIRE\',\'SWOPSTORE\') and upper(fr.final_utm_medium) like \'%AFFILIATE%\' then fr.final_utm_campaign else fr.final_utm_medium end final_utm_medium, CASE WHEN lower(partner_af) = \'grabon\' then 80 WHEN lower(partner_af) = \'admitad\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'admitad\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'pinchpennies\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'kickcash\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'adcanapous\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'cred\' and coalesce(frt.order_date, fr.ordeR_date) <= \'2024-04-30\' and lower(customer_flag) = \'new\' then 200 WHEN lower(partner_af) = \'cred\' and coalesce(frt.order_date, fr.ordeR_date) <= \'2024-04-30\' and lower(customer_flag) = \'repeated\' then 100 WHEN lower(partner_af) = \'wishlink\'and lower(customer_flag) = \'new\' then 0.18*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'wishlink\'and lower(customer_flag) = \'repeated\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'unstop\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'icubewire\' and coalesce(frt.order_date, fr.ordeR_date) between \'2023-11-15\' and \'2023-12-15\' then 0.09*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'icubewire\' and coalesce(frt.order_date, fr.ordeR_date) between \'2024-02-01\' and \'2024-03-31\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'icubewire\' and coalesce(frt.order_date, fr.ordeR_date) > \'2024-04-01\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'icubewire\' and coalesce(frt.order_date, fr.ordeR_date) > \'2024-04-01\' and lower(customer_flag) = \'repeated\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'icubewire\' then 0.07*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'jio\' then 200 WHEN lower(partner_af) = \'clickonik\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'clickonik\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'mydala\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'mydala\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'vcommission\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'vcommission\'and lower(customer_flag) = \'repeated\' then 0.06*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'adsflourish\'and lower(customer_flag) = \'new\' then 0.07*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'adsflourish\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'twid\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'twid\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'mobilog\' then 0.06*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'rohit\' and coalesce(frt.order_date, fr.ordeR_date) > \'2024-04-01\' and coalesce(frt.order_date, fr.ordeR_date) < \'2024-06-01\' and lower(customer_flag) = \'new\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'rohit\' and coalesce(frt.order_date, fr.ordeR_date) > \'2024-04-01\' and coalesce(frt.order_date, fr.ordeR_date) < \'2024-06-01\' and lower(customer_flag) = \'repeated\' then 0.06*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'rohit\' and coalesce(frt.order_date, fr.ordeR_date) >= \'2024-06-01\' and lower(customer_flag) = \'new\' then 0.11*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'rohit\' and coalesce(frt.order_date, fr.ordeR_date) >= \'2024-06-01\' and lower(customer_flag) = \'repeated\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'rohit\' then 0.07*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'swopstore\' and lower(customer_flag) = \'new\' then 0.07*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'swopstore\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'filly media\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'filly media\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'affmantra\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'affmantra\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'affle\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'affle\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'prudentads\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'prudentads\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'analyticsclouds\' and lower(customer_flag) = \'new\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'analyticsclouds\' and lower(customer_flag) = \'repeated\' then 0.06*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'ajm\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'ajm\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'monetizedeal\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'monetizedeal\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'shoogloo\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'shoogloo\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'affiliate_cr\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'octaads\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'octaads\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'tmdf\' and lower(customer_flag) = \'new\' then 0.11*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'tmdf\' and lower(customer_flag) = \'repeated\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'click2commission\' then 0.06*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'flickstree\' and lower(customer_flag) = \'new\' then 0.11*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'flickstree\' and lower(customer_flag) = \'repeated\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'affnads\' and lower(customer_flag) = \'new\' then 0.11*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'affnads\' and lower(customer_flag) = \'repeated\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'tdmf\' and lower(customer_flag) = \'new\' then 0.11*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'tdmf\' and lower(customer_flag) = \'repeated\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'adgama\' and lower(customer_flag) = \'new\' then 0.11*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'adgama\' and lower(customer_flag) = \'repeated\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'mmpl\' and lower(customer_flag) = \'new\' then 0.11*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(partner_af) = \'mmpl\' and lower(customer_flag) = \'repeated\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float end cost, COALESCE(SUM(fr.total_sales), 0) AS sales, COALESCE(SUM(fr.total_discount), 0) AS discount, COALESCE(SUM(fr.dto_sales), 0) as dto_sales, COALESCE(SUM(fr.rto_sales), 0) as rto_sales, COALESCE(SUM(case when fr.cancelled_sales is null then frt.return_sales end), 0) AS returns, COALESCE(SUM(frt.cancelled_sales),sum(fr.cancelled_sales), 0) AS cancelled, NULL AS utm_flag, \'discount_code\' AS source_type FROM affiliates af LEFT JOIN fact_revenue fr ON LOWER(af.discount_code) = LOWER(fr.discount_code) LEFT JOIN fact_returns frt ON fr.order_id = frt.order_id AND fr.line_item_id = SPLIT_PART(frt.saleorderitemcode, \'-\', 1) WHERE af.discount_code IS NOT NULL or lower(tags) like \'%wishlink%\' GROUP BY 1, 2, 3, 4, 5,6,7,8,9,10,11 ), utm_source_merge AS ( SELECT coalesce(frt.order_date, fr.ordeR_date) ordeR_date, upper(fr.discount_code) as discount_code, fr.order_name, fr.tags, upper(trim(fr.final_utm_source)) as source_fr, upper(trim(fr.final_utm_source)) as partner_fr, fr.phone, fr.email, fr.customer_flag, fr.web_app_flag, case when source_fr = \'VCOMMISSION\' and frt.order_date::date > \'2023-12-19\' then fr.final_utm_campaign when source_fr = \'CLICKONIK\' and frt.order_date::date >= \'2023-12-31\' then fr.final_utm_campaign when source_fr = \'ICUBEWIRE\' and frt.order_date::date >= \'2024-01-04\' then fr.final_utm_campaign when source_fr = \'SWOPSTORE\' and frt.order_date::date >= \'2023-12-25\' then fr.final_utm_campaign when source_fr not in (\'VCOMMISSION\',\'CLICKONIK\',\'ICUBEWIRE\',\'SWOPSTORE\') and upper(fr.final_utm_medium) like \'%AFFILIATE%\' then fr.final_utm_campaign else fr.final_utm_medium end final_utm_medium, CASE WHEN lower(source_fr) = \'grabon\' then 80 WHEN lower(source_fr) = \'admitad\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'admitad\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'pinchpennies\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'kickcash\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'adcanapous\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'cred\' and coalesce(frt.order_date, fr.ordeR_date) <= \'2024-04-30\' and lower(customer_flag) = \'new\' then 200 WHEN lower(source_fr) = \'cred\' and coalesce(frt.order_date, fr.ordeR_date) <= \'2024-04-30\' and lower(customer_flag) = \'repeated\' then 100 WHEN lower(source_fr) = \'wishlink\'and lower(customer_flag) = \'new\' then 0.18*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'wishlink\'and lower(customer_flag) = \'repeated\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'unstop\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'icubewire\' and coalesce(frt.order_date, fr.ordeR_date) between \'2023-11-15\' and \'2023-12-15\' then 0.09*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'icubewire\' and coalesce(frt.order_date, fr.ordeR_date) between \'2024-02-01\' and \'2024-03-31\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'icubewire\' and coalesce(frt.order_date, fr.ordeR_date) > \'2024-04-01\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'icubewire\' and coalesce(frt.order_date, fr.ordeR_date) > \'2024-04-01\' and lower(customer_flag) = \'repeated\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'icubewire\' then 0.07*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'jio\' then 200 WHEN lower(source_fr) = \'clickonik\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'clickonik\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'mydala\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'mydala\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'vcommission\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'vcommission\'and lower(customer_flag) = \'repeated\' then 0.06*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'adsflourish\'and lower(customer_flag) = \'new\' then 0.07*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'adsflourish\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'twid\'and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'twid\'and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'mobilog\' then 0.06*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'rohit\' and coalesce(frt.order_date, fr.ordeR_date) > \'2024-04-01\' and coalesce(frt.order_date, fr.ordeR_date) < \'2024-06-01\' and lower(customer_flag) = \'new\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'rohit\' and coalesce(frt.order_date, fr.ordeR_date) > \'2024-04-01\' and coalesce(frt.order_date, fr.ordeR_date) < \'2024-06-01\' and lower(customer_flag) = \'repeated\' then 0.06*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'rohit\' and coalesce(frt.order_date, fr.ordeR_date) >= \'2024-06-01\' and lower(customer_flag) = \'new\' then 0.11*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'rohit\' and coalesce(frt.order_date, fr.ordeR_date) >= \'2024-06-01\' and lower(customer_flag) = \'repeated\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'rohit\' then 0.07*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'swopstore\' and lower(customer_flag) = \'new\' then 0.07*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'swopstore\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'filly media\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'filly media\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'affmantra\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'affmantra\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'affle\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'affle\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'prudentads\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'prudentads\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'analyticsclouds\' and lower(customer_flag) = \'new\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'analyticsclouds\' and lower(customer_flag) = \'repeated\' then 0.06*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'ajm\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'ajm\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'monetizedeal\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'monetizedeal\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'shoogloo\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'shoogloo\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'affiliate_cr\' then 0.1*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'octaads\' and lower(customer_flag) = \'new\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'octaads\' and lower(customer_flag) = \'repeated\' then 0.05*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'tmdf\' and lower(customer_flag) = \'new\' then 0.11*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'tmdf\' and lower(customer_flag) = \'repeated\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'click2commission\' then 0.06*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'flickstree\' and lower(customer_flag) = \'new\' then 0.11*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'flickstree\' and lower(customer_flag) = \'repeated\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'affnads\' and lower(customer_flag) = \'new\' then 0.11*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'affnads\' and lower(customer_flag) = \'repeated\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'tdmf\' and lower(customer_flag) = \'new\' then 0.11*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'tdmf\' and lower(customer_flag) = \'repeated\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'adgama\' and lower(customer_flag) = \'new\' then 0.11*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'adgama\' and lower(customer_flag) = \'repeated\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'mmpl\' and lower(customer_flag) = \'new\' then 0.11*COALESCE(SUM(fr.total_sales), 0)::float WHEN lower(source_fr) = \'mmpl\' and lower(customer_flag) = \'repeated\' then 0.08*COALESCE(SUM(fr.total_sales), 0)::float end cost, COALESCE(SUM(fr.total_sales), 0) AS sales, COALESCE(SUM(fr.total_discount), 0) AS discount, COALESCE(SUM(fr.dto_sales), 0) as dto_sales, COALESCE(SUM(fr.rto_sales), 0) as rto_sales, COALESCE(SUM(case when fr.cancelled_sales is null then frt.return_sales end), 0) AS returns, COALESCE(SUM(frt.cancelled_sales),sum(fr.cancelled_sales), 0) AS cancelled, fm.utm_flag, \'utm_source\' AS source_type FROM fact_revenue fr LEFT JOIN fact_merge fm ON LOWER(fm.source) = lower(fr.final_utm_source) LEFT JOIN fact_returns frt ON fr.order_id = frt.order_id AND fr.line_item_id = SPLIT_PART(frt.saleorderitemcode, \'-\', 1) WHERE fm.utm_flag = 1 or lower(tags) like \'%wishlink%\' GROUP BY 1, 2, 3, 4, 5,6,7,8, 9,10,11,fm.utm_flag ), union_merge AS ( SELECT * FROM discount_code_merge UNION SELECT * FROM utm_source_merge ) SELECT * FROM union_merge um LEFT JOIN fact_monthly_target fmt on date_trunc(\'month\',um.order_date::Date) = date_trunc(\'month\',fmt.date::date) and lower(um.partner_af) = lower(fmt.src) ; create or replace table snitch_db.maplemonk.affiliate_snitch as select order_date, discount_code, ordeR_name, tags, partner_af, source_af, phone, email, customer_flag, web_app_flag, final_utm_medium, ifnull(a.cost,0) + ifnull(div0(b.cost,count(1) over (partition by date_trunc(month, a.order_date),coalesce(partner_af,source_af) )),0) cost, sales, discount, dto_sales, rto_sales, returns, cancelled, utm_flag, date, src, monthly_cost, monthly_target, source_type from snitch_db.maplemonk.affiliate_snitch_intermediate a left join snitch_db.maplemonk.partner_monthly_spends b on date_trunc(month, a.order_date) = b.month and lower(coalesce(partner_af,source_af)) = lower(b.partner) where order_Date <= \'2024-04-30\' union all select order_date, discount_code, ordeR_name, tags, partner_af, source_af, phone, email, customer_flag, web_app_flag, final_utm_medium, ifnull(a.cost,0) + ifnull(div0(b.cost,count(1) over (partition by date_trunc(month, a.order_date),coalesce(partner_af,source_af) )),0) cost, sales, discount, dto_sales, rto_sales, returns, cancelled, utm_flag, date, src, monthly_cost, monthly_target, source_type from ( select * from ( select order_date, discount_code, ordeR_name, tags, partner_af, source_af, phone, email, customer_flag, web_app_flag, final_utm_medium, cost, sales, discount, dto_sales, rto_sales, returns, cancelled, utm_flag, date, src, monthly_cost, monthly_target, source_type, row_number() over (partition by ordeR_name order by source_type asc) rw from snitch_db.maplemonk.affiliate_snitch_intermediate ) where rw=1 and order_Date >= \'2024-05-01\') a left join snitch_db.maplemonk.partner_monthly_spends b on date_trunc(month, a.order_date) = b.month and lower(coalesce(partner_af,source_af)) = lower(b.partner) ; create or replace table snitch_db.maplemonk.affiliate_cod as ( with affiliate as ( select distinct order_name,sales,discount,partner_af from snitch_db.maplemonk.affiliate_snitch ), row_num as ( select *, ROW_NUMBER() OVER (PARTITION BY pincode ORDER BY \"Office Name\" DESC) AS row_num from snitch_db.maplemonk.pincode_mapping ), sales_data as ( select distinct a.order_name,a.order_timestamp::date as order_date,a.payment_channel,a.pincode,b.statename from snitch_db.maplemonk.fact_items_snitch a left join row_num b on a.pincode = b.pincode where b.row_num = 1 ) select a.partner_af,a.sales,a.discount,b.* from affiliate a inner join sales_data b on a.order_name = b.order_name );",
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
            