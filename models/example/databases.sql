{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.retailer_replacement as SELECT sku AS \"sku\", DATE_TRUNC(\'DAY\', date) AS \"date\", retailer_name AS \"retailer_name\", area AS \"area\", case when area in (\'NCR-MT\', \'Delhi-GT\', \'Noida-GT\', \'Gurgaon-GT\', \'NCR-HORECA\', \'NCR-ON-MT\', \'NCR-OF-MT\') then \'1.NCR\' when area in (\'NCR-UB\', \'Bangalore-UB\', \'UP-UB\') then \'6.UB\' when area in (\'Bangalore-MT\', \'Bangalore-GT\') then \'2.Bangalore\' when area in (\'Bangalore-UB\') then \'7.Bangalore-UB\' when area in (\'Allahabad-GT\', \'Lucknow-GT\', \'UP-MT\') then \'4.UP\' when area in (\'UP-UB\') then \'8.UP-UB\' when area in (\'Indore-GT\', \'Bhopal-GT\', \'MP-MT\') then \'3.MP\' when area in (\'East-MT\') then \'5.East\' else \'9.Others\' end AS \"Area Aggregation\", sum(eggs_sold) AS \"Eggs_Sold\", sum(eggs_replaced) AS \"Eggs_Replaced\", case when sum(eggs_sold) = 0 then 0 else 100*sum(eggs_replaced)/sum(eggs_sold) END AS \"Replacement%\", sum(sku2.sku_total_replaced) FROM maplemonk.summary_reporting_table_beat_retailer_sku sku1 join (select DATE_TRUNC(\'DAY\', date) AS \"date2\", area as \"area_\", retailer_name as \"name\", sum(eggs_replaced) as sku_total_replaced from maplemonk.summary_reporting_table_beat_retailer_sku group by DATE_TRUNC(\'DAY\', date), \"area_\", \"name\") sku2 on sku1.date = sku2.\"date2\" and sku1.area = sku2.\"area_\" and sku1.retailer_name = sku2.\"name\" GROUP BY sku, DATE_TRUNC(\'DAY\', date), retailer_name, area, case when area in (\'NCR-MT\', \'Delhi-GT\', \'Noida-GT\', \'Gurgaon-GT\', \'NCR-HORECA\', \'NCR-ON-MT\', \'NCR-OF-MT\') then \'1.NCR\' when area in (\'NCR-UB\', \'Bangalore-UB\', \'UP-UB\') then \'6.UB\' when area in (\'Bangalore-MT\', \'Bangalore-GT\') then \'2.Bangalore\' when area in (\'Bangalore-UB\') then \'7.Bangalore-UB\' when area in (\'Allahabad-GT\', \'Lucknow-GT\', \'UP-MT\') then \'4.UP\' when area in (\'UP-UB\') then \'8.UP-UB\' when area in (\'Indore-GT\', \'Bhopal-GT\', \'MP-MT\') then \'3.MP\' when area in (\'East-MT\') then \'5.East\' else \'9.Others\' end ORDER BY \"Eggs_Sold\" DESC ;",
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
                        