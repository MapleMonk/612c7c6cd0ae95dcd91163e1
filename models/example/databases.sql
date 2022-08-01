{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.weighted_sell AS SELECT area, beat_number_original, retailer_name , retailer_commission, sku, CASE WHEN \"Weighted eggs sold\"<0 THEN 0 ELSE \"Weighted eggs sold\" END AS \"Weighted eggs sold\" FROM (SELECT area, beat_number_original, retailer_name , retailer_commission, sku, CASE WHEN \"Net_Eggs_sold\"<=0 THEN 0 WHEN DATE_TRUNC(\'DAY\', date)>=\'2022-08-15 00:00:00\' AND DATE_TRUNC(\'DAY\', date)<=\'2022-08-16 00:00:00\' THEN ((sum(\"Util Eggs sold\") over (partition by retailer_name, sku))/(sum(\"Weight\") over (partition by retailer_name, sku)))*1.6 ELSE ((sum(\"Util Eggs sold\") over (partition by retailer_name, sku))/(sum(\"Weight\") over (partition by retailer_name, sku)))*1.2 END AS \"Weighted eggs sold\" FROM (SELECT date, sku , area, beat_number_original, days_since_onboard , retailer_name , retailer_commission, (sum(eggs_sold)-sum(eggs_return)) AS \"Net_Eggs_sold\", CASE WHEN DATE_TRUNC(\'DAY\', date)<DATEADD(day, -28, GETDATE()) AND days_since_onboard>=28 THEN 1 WHEN DATE_TRUNC(\'DAY\', date)>=DATEADD(day, -28, GETDATE()) AND DATE_TRUNC(\'DAY\', date)<DATEADD(day, -14, GETDATE()) AND days_since_onboard>=14 THEN 4 WHEN DATE_TRUNC(\'DAY\', date)>=DATEADD(day, -14, GETDATE()) AND DATE_TRUNC(\'DAY\', date)<GETDATE() THEN 9 END AS \"Weight\", CASE WHEN sum(eggs_sold)<sum(eggs_return) THEN 0 WHEN DATE_TRUNC(\'DAY\', date)<DATEADD(day, -28, GETDATE()) THEN sum(eggs_sold)-sum(eggs_return) WHEN DATE_TRUNC(\'DAY\', date)>=DATEADD(day, -28, GETDATE()) AND DATE_TRUNC(\'DAY\', date)<DATEADD(day, -14, GETDATE()) THEN 4*(sum(eggs_sold)-sum(eggs_return)) WHEN DATE_TRUNC(\'DAY\', date)>=DATEADD(day, -14, GETDATE()) AND DATE_TRUNC(\'DAY\', date)<GETDATE() THEN 9*(sum(eggs_sold)-sum(eggs_return)) END AS \"Util Eggs sold\" FROM maplemonk.summary_reporting_table_beat_retailer_sku WHERE date >= DATEADD(day, -60, GETDATE()) AND date < GETDATE() AND area IN (\'Delhi-GT\',\'Gurgaon-GT\',\'Noida-GT\',\'NCR-MT\') GROUP BY date, sku, area, beat_number_original, days_since_onboard, retailer_name, retailer_commission ) ) GROUP BY area, beat_number_original, retailer_name, retailer_commission, sku, \"Weighted eggs sold\" ; CREATE OR REPLACE TABLE eggozdb.maplemonk.test AS SELECT * FROM (SELECT area, beat_number_original, retailer_name, retailer_commission, sku, CASE WHEN (sum((\"Egg_sold_type\")*(\"New_VWS%\")*(\"Profit_per_egg\")) over (partition by retailer_name)) > (sum((\"Egg_sold_type\")*(\"VS%\")*(\"Profit_per_egg\")) over (partition by retailer_name)) THEN (\"Egg_sold_type\")*(\"New_VWS%\") ELSE (\"Egg_sold_type\")*(\"VS%\") END AS \"New_Demand\" FROM (SELECT area, beat_number_original, retailer_name, retailer_commission, sku, sum(\"Weighted eggs sold\") over (partition by retailer_name, \"Egg_type\") AS \"Egg_sold_type\", \"Profit_per_egg\", CASE WHEN \"Weighted eggs sold\"=0 THEN 0 ELSE (sum(\"Weighted eggs sold\") over (partition by sku, retailer_name))/(sum(\"Weighted eggs sold\") over (partition by \"Egg_type\",retailer_name)) END AS \"VS%\", CASE WHEN \"Weighted eggs sold\"=0 THEN 0 ELSE (sum(\"Profit_per_egg\") over(partition by sku,retailer_name))/(sum(\"Profit_per_egg\") over (partition by \"Egg_type\",retailer_name)) END AS \"UTIL\", CASE WHEN \"Weighted eggs sold\"=0 THEN 0 ELSE 0.8*(sum(\"Weighted eggs sold\") over (partition by sku,retailer_name))/(sum(\"Weighted eggs sold\") over (partition by \"Egg_type\",retailer_name))+0.2*(sum(\"Profit_per_egg\") over(partition by sku,retailer_name))/(sum(\"Profit_per_egg\") over (partition by \"Egg_type\",retailer_name)) END AS \"New_VWS%\" FROM( SELECT sku , area , beat_number_original , retailer_name , retailer_commission , \"Weighted eggs sold\", CASE WHEN \"Weighted eggs sold\"=0 THEN 0 WHEN sku IN (\'6B\') THEN (100-retailer_commission)/100*18.3-8.7 WHEN sku IN (\'10B\') THEN (100-retailer_commission)/100*18.0-8.6 WHEN sku IN (\'25B\') THEN (100-retailer_commission)/100*15.2-8.3 WHEN sku IN (\'30B\') THEN (100-retailer_commission)/100*15.0-8.1 WHEN sku IN (\'6W\') THEN (100-retailer_commission)/100*14.2-7.7 WHEN sku IN (\'10W\') THEN (100-retailer_commission)/100*14.0-7.6 WHEN sku IN (\'25W\') THEN (100-retailer_commission)/100*12.0-7.3 WHEN sku IN (\'30W\') THEN (100-retailer_commission)/100*11.0-7.1 WHEN sku IN (\'10N\') THEN (100-retailer_commission)/100*15.5-8.2 END AS \"Profit_per_egg\", CASE WHEN sku IN(\'6B\',\'10B\',\'25B\',\'30B\') THEN \'Brown\' WHEN sku IN(\'6W\',\'10W\',\'25W\',\'30W\') THEN \'White\' WHEN sku IN(\'10N\') THEN \'Nutra\' END AS \"Egg_type\" FROM eggozdb.maplemonk.weighted_sell GROUP BY sku, area, beat_number_original, retailer_name, retailer_commission, \"Weighted eggs sold\" ) ) ) WHERE \"New_Demand\"<>0; CREATE OR REPLACE TABLE eggozdb.maplemonk.next_day_demand AS SELECT dcba.beat_date,t.* FROM eggozdb.maplemonk.test t JOIN eggozdb.maplemonk.my_sql_distributionchain_beatassignment dcba ON t.beat_number_original=dcba.beat_number WHERE dcba.beat_date=DATEADD(day,1,GETDATE()::date) ORDER BY retailer_name",
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
                        