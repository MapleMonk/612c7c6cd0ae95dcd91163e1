{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table redtape_db.maplemonk.redtape_db_inventory_report as select \"Report Generated Date\"::date Date, case when upper(location) = \'REDTAPE LIMITED\' then \'NOIDA\' when upper(location) like \'%HOWRAH%\' then \'HOWRAH\' when upper(location) like \'%UNNAO%\' then \'UNNAO\' when upper(location) like \'%MUMBAI%\' then \'MUMBAI\' when upper(location) like \'%NOIDA%\' then \'NOIDA\' when upper(location) like \'%KASHIPUR%\' then \'KASHIPUR\' when upper(location) like \'%HYDERABAD%\' then \'HYDERABAD\' end as location, replace(a.sku,\'`\',\'\') sku, REGEXP_REPLACE(replace(a.sku,\'`\',\'\'), \'-[^-]*$\', \'\') article, pgd.division, pgd.gender, coalesce(pma.category,pmf.category,pmac.category) category, m.season, replace(a.EAN,\'`\',\'\') EAN, \"Reserved (Not Picked)\" reserved_not_picked, \"Reserved (Picked)\" reserved_picked, \"Available Quantity\" available_quantity, \"Available Quantity (Bin Locked)\" bin_locked_quantity, m.category_a_b_C from redtape_db.maplemonk.easyecom_redtape_inventory_snapshot a left join (select * from (select *, row_number() over (partition by code order by 1) rw from redtape_db.maplemonk.product_mapping_gender_division) where rw=1) pgd on upper(left(replace(a.SKU,\'`\',\'\'),3)) = pgd.code left join (select * from (select code, \"FOR SELL THRU\" category, row_number() over (partition by code order by 1) rw from redtape_db.maplemonk.product_mapping_apparel) where rw = 1) pma on upper(left(replace(a.SKU,\'`\',\'\'),3)) = pma.code left join (select * from (select code, gategory category, row_number() over (partition by code order by 1) rw from redtape_db.maplemonk.product_mapping_footwear) where rw = 1) pmf on upper(left(replace(a.SKU,\'`\',\'\'),3)) = pmf.code left join (select * from (select code, category, row_number() over (partition by code order by 1) rw from redtape_db.maplemonk.product_mapping_accessories) where rw = 1) pmac on upper(left(replace(a.SKU,\'`\',\'\'),3)) = pmac.code left join (select * from ( select article, \"Category A,B,C\" category_a_b_c, season, row_number() over (partition by article order by 1) rw from redtape_db.maplemonk.product_categoryabc_mapping )where rw = 1) m on upper(m.article) = upper(REGEXP_REPLACE(replace(a.sku,\'`\',\'\'), \'-[^-]*$\', \'\')) ;",
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
            