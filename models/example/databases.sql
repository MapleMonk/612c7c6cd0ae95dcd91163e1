{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table redtape_db.maplemonk.redtape_db_inventory_report as select DATA_FETCH_DATE::date Date, case when upper(COMPANYNAME) = \'REDTAPE LIMITED\' then \'NOIDA\' when upper(COMPANYNAME) like \'%HOWRAH%\' then \'HOWRAH\' when upper(COMPANYNAME) like \'%UNNAO%\' then \'UNNAO\' when upper(COMPANYNAME) like \'%MUMBAI%\' then \'MUMBAI\' when upper(COMPANYNAME) like \'%NOIDA%\' then \'NOIDA\' when upper(COMPANYNAME) like \'%KASHIPUR%\' then \'KASHIPUR\' when upper(COMPANYNAME) like \'%HYDERABAD%\' then \'HYDERABAD\' when lower(COMPANYNAME) like \'%bahadurgarh%\' then \'BAHADURGARH\' end as location, replace(a.sku,\'`\',\'\') sku, REGEXP_REPLACE(replace(a.sku,\'`\',\'\'), \'-[^-]*$\', \'\') article, pgd.division, pgd.gender, coalesce(pma.category,pmf.category,pmac.category) category, m.season, RESERVEDINVENTORY, AVAILABLEINVENTORY available_quantity, m.category_a_b_C from (select * from redtape_db.maplemonk.easyecom_redtape_inventory_details qualify row_number() over(partition by sku,data_fetch_date::date order by DATA_FETCH_DATE desc) = 1) a left join (select * from (select *, row_number() over (partition by code order by 1) rw from redtape_db.maplemonk.product_mapping_gender_division) where rw=1) pgd on upper(left(replace(a.SKU,\'`\',\'\'),3)) = pgd.code left join (select * from (select code, \"FOR SELL THRU\" category, row_number() over (partition by code order by 1) rw from redtape_db.maplemonk.product_mapping_apparel) where rw = 1) pma on upper(left(replace(a.SKU,\'`\',\'\'),3)) = pma.code left join (select * from (select code, gategory category, row_number() over (partition by code order by 1) rw from redtape_db.maplemonk.product_mapping_footwear) where rw = 1) pmf on upper(left(replace(a.SKU,\'`\',\'\'),3)) = pmf.code left join (select * from (select code, category, row_number() over (partition by code order by 1) rw from redtape_db.maplemonk.product_mapping_accessories) where rw = 1) pmac on upper(left(replace(a.SKU,\'`\',\'\'),3)) = pmac.code left join (select * from ( select article, \"Category A,B,C\" category_a_b_c, season, row_number() over (partition by article order by 1) rw from redtape_db.maplemonk.product_categoryabc_mapping )where rw = 1) m on upper(m.article) = upper(REGEXP_REPLACE(replace(a.sku,\'`\',\'\'), \'-[^-]*$\', \'\')) ;",
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
            