{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.ub_log as select tt.*, grn_date, ifnull(t2.procured_region,tt.region) procured_region, ifnull(t2.procured_category,tt.category) procured_category, ifnull(t2.procured_amount_daily,0) procured_amount_daily, ifnull(t2.procured_eggs_daily,0) procured_eggs_daily, ifnull(t2.procured_price_daily, avg(t2.procured_price_daily) over (partition by year(tt.delivery_date), month(tt.delivery_date), tt.region, tt.category order by tt.delivery_date)) procured_price_daily from ( select * from ( select delivery_date, egg_type, category, region, sum(eggs_sold) over (partition by egg_type, category, region, delivery_date order by delivery_date) eggs_sold_daily_typewise, sum(sale) over (partition by egg_type, category, region, delivery_date order by delivery_date) sales_daily_typewise, sum(sale) over (partition by egg_type, category, region, delivery_date order by delivery_date)/iff(sum(eggs_sold) over (partition by egg_type, category, region, delivery_date order by delivery_date)=0,1,sum(eggs_sold) over (partition by egg_type, category, region, delivery_date order by delivery_date)) SP_Daily_typewise, sum(eggs_sold) over (partition by category, region, delivery_date order by delivery_date) eggs_sold_daily_categorywise, sum(sale) over (partition by category, region, delivery_date order by delivery_date) sales_daily_categorywise, sum(sale) over (partition by category, region, delivery_date order by delivery_date)/iff(sum(eggs_sold) over (partition by egg_type, category, region, delivery_date order by delivery_date)=0,1,sum(eggs_sold) over (partition by egg_type, category, region, delivery_date order by delivery_date)) SP_Daily_categorywise, row_number() over (partition by delivery_date, egg_type, category, region order by delivery_date) rownumber from ( SELECT cast(timestampadd(minute,660,oo.delivery_date ) as date) as delivery_date ,ot.egg_type , case when lower(pp.name) like \'%white%\' then \'White\' when lower(pp.name) like \'%brown%\' then \'Brown\' when lower(pp.name) like \'%nutra%\' then \'White\' when lower(pp.name) like \'%liquid%\' then \'White\' end as category , case when rr.area_classification = \'Bangalore-UB\' then \'Bangalore\' when rr.area_classification = \'NCR-UB\' then \'NCR\' when rr.area_classification = \'MP-UB\' then \'M.P\' when rr.area_classification = \'UP-UB\' then \'U.P\' when rr.area_classification = \'East-UB\' then \'East\' end as region , rr.area_classification , concat(pp.sku_count,pp.name) as SKU , sum(ot.quantity) as quantity , sum(case when rr.area_classification = \'UP-UB\' then ot.quantity * pp.SKU_Count else (case when lower(pp.name) like \'%liquid%\' then (ot.quantity*1000)/35 when SKU = \'1White\' then ot.quantity * pp.SKU_Count * 30 when SKU = \'1Brown\' then ot.quantity * pp.SKU_Count * 30 else ot.quantity * pp.SKU_Count end) end) as eggs_sold , ot.single_sku_rate , ot.single_sku_rate*sum(ot.quantity) as sale , pp.SKU_Count FROM eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_order_orderline ot on ot.order_id = oo.id left join eggozdb.maplemonk.my_sql_product_product pp on ot.product_id = pp.id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id where lower(secondary_status) <> \'cancel_approved\' and rr.area_classification like \'%UB%\' and lower(status) <> \'cancelled\' and cast(timestampadd(minute,660,oo.delivery_date ) as date) between \'2022-08-01\' and (cast(timestampadd(minute, 660, getdate()) as date)-1) group by ot.egg_type, category, region ,delivery_date , rr.area_classification , ot.single_sku_rate , ot.single_sku_mrp , pp.SKU_Count , pp.name union all SELECT cast(timestampadd(minute,660,oo.delivery_date ) as date) as delivery_date ,case when lower(pp.slug) like \'%wd%\' then \'Darjan\' end as egg_type , case when lower(pp.name) like \'%white%\' then \'White\' when lower(pp.name) like \'%brown%\' then \'Brown\' when lower(pp.name) like \'%nutra%\' then \'White\' when lower(pp.name) like \'%liquid%\' then \'White\' end as category , CASE WHEN rr.area_classification IN (\'Gurgaon-GT\',\'Delhi-GT\',\'NCR-OF-MT\',\'Noida-GT\',\'NCR-MT\',\'NCR-ON-MT\',\'NCR-HORECA\') THEN \'NCR\' WHEN rr.area_classification IN(\'Allahabad-GT\',\'Lucknow-GT\',\'UP-MT\',\'UP-ON-MT\',\'UP-OF-MT\') THEN \'U.P\' WHEN rr.area_classification IN(\'Indore-GT\',\'Bhopal-GT\',\'MP-ON-MT\',\'MP-OF-MT\') THEN \'M.P\' WHEN rr.area_classification IN(\'Bangalore-Horeca\',\'Bangalore-MT\',\'Bangalore-GT\',\'Bangalore-ON-MT\',\'Bangalore-OF-MT\') THEN \'Bangalore\' WHEN rr.area_classification IN(\'East-MT\',\'East-ON-MT\',\'East-Kol-MT\', \'East-OF-MT\', \'Patna-GT\',\'Kolkata-GT\') THEN \'East\' ELSE \'Others\' end as region , rr.area_classification , concat(pp.sku_count,pp.name) as SKU , ot.quantity as quantity , ot.quantity * pp.SKU_Count as eggs_sold , ot.single_sku_rate , ot.single_sku_rate*ot.quantity as sale , pp.SKU_Count FROM eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_order_orderline ot on ot.order_id = oo.id left join eggozdb.maplemonk.my_sql_product_product pp on ot.product_id = pp.id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id where lower(secondary_status) <> \'cancel_approved\' and lower(pp.slug) like \'%wd%\' and lower(status) in (\'completed\',\'delivered\') and pp.brand_type = \'branded\' and cast(timestampadd(minute,660,oo.delivery_date ) as date) between \'2022-08-01\' and (cast(timestampadd(minute, 660, getdate()) as date)-1) group by ot.egg_type, category, region, ot.quantity ,delivery_date , rr.area_classification , ot.single_sku_rate , ot.single_sku_mrp , pp.SKU_Count , pp.name , pp.slug )) where rownumber = 1 ) tt full outer join (select grn_date, region as procured_region, case when lower(type) in (\'nutra+\',\'chataki\',\'chatki\',\'white\',\'darjan\') then \'White\' when lower(type) in (\'brown\') then \'Brown\' else \'others\' end as procured_category, sum(replace(amount,\',\',\'\')::FLOAT) as procured_amount_daily, sum(replace(eggs,\',\',\'\')::FLOAT) as procured_eggs_daily, sum(replace(amount,\',\',\'\')::FLOAT)/iff(sum(replace(eggs,\',\',\'\')::FLOAT)=0,1,sum(replace(eggs,\',\',\'\')::FLOAT)) as procured_price_daily from eggozdb.maplemonk.region_wise_procurement_masterdata group by grn_date, procured_region, procured_category ) t2 on tt.delivery_date = t2.grn_date and tt.category = t2.procured_category and tt.region = t2.procured_region ;",
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
                        