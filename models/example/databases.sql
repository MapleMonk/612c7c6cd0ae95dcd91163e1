{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table redtape_db.maplemonk.redtape_db_sellthrough as select c.* ,d.sale_quantity ,e.return_quantity from (select week_start - 7 week_start, category_mapped, division, gender, replace(SKU,\'`\',\'\') sku, REGEXP_REPLACE(replace(SKU,\'`\',\'\'), \'-[^-]*$\', \'\') article, sum(\"Available Quantity\") closing_inventory from ( select s.* ,pgd.division ,pgd.gender ,coalesce(pma.category,pmf.category,pmac.category) category_mapped from (select m.*,a.week_Start from (select distinct date_trunc(week,ordeR_date::Date) week_start from redtape_db.Maplemonk.redtape_db_sales_consolidated)a left join redtape_db.maplemonk.easyecom_redtape_inventory_snapshot m on a.week_start = m.\"Report Generated Date\"::date + 1 where m.\"Report Generated Date\"::date is not null ) s left join (select * from (select *, row_number() over (partition by code order by 1) rw from redtape_db.maplemonk.product_mapping_gender_division) where rw=1) pgd on upper(left(replace(s.SKU,\'`\',\'\'),3)) = pgd.code left join (select * from (select code, \"FOR SELL THRU\" category, row_number() over (partition by code order by 1) rw from redtape_db.maplemonk.product_mapping_apparel) where rw = 1) pma on upper(left(replace(s.SKU,\'`\',\'\'),3)) = pma.code left join (select * from (select code, gategory category, row_number() over (partition by code order by 1) rw from redtape_db.maplemonk.product_mapping_footwear) where rw = 1) pmf on upper(left(replace(s.SKU,\'`\',\'\'),3)) = pmf.code left join (select * from (select code, category, row_number() over (partition by code order by 1) rw from redtape_db.maplemonk.product_mapping_accessories) where rw = 1) pmac on upper(left(replace(s.SKU,\'`\',\'\'),3)) = pmac.code ) group by 1,2,3,4,5,6 ) c left join (select category_mapped, division, gender, sku,date_trunc(week,order_Date::Date) week_Start, sum(quantity) sale_quantity from redtape_db.maplemonk.redtape_db_sales_consolidated group by 1,2,3,4,5 ) d on c.gender = d.gender and c.division = d.division and c.category_mapped = d.category_mapped and c.week_Start = d.week_Start and c.sku = d.sku left join (select category_mapped, division, gender, sku,date_trunc(week,return_date::Date) week_Start, sum(total_returned_quantity) return_quantity from redtape_db.maplemonk.redtape_db_returns_consolidated group by 1,2,3,4,5 ) e on c.gender = e.gender and c.division = e.division and c.category_mapped = e.category_mapped and c.week_Start = e.week_Start and c.sku = e.sku",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from REDTAPE_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            