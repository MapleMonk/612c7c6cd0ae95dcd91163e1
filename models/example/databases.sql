{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.MEDMONGERS_FINAL_SKU_MASTER AS select cast(sm.mrp as float) as mrp, upper(cast(trim(\"TALLY SKU\") as varchar)) as sku_code, upper(trim(\"Portal Name\")) as channelname, CASE WHEN upper(trim(\"Portal Name\")) LIKE \'%FLIPKART%\' THEN \'FLIPKART\' WHEN upper(trim(\"Portal Name\")) LIKE \'%AMAZON%\' THEN \'AMAZON\' WHEN upper(trim(\"Portal Name\")) LIKE \'%SNAPDEAL%\' THEN \'SNAPDEAL\' WHEN upper(trim(\"Portal Name\")) LIKE \'%PHARMEASY%\' THEN \'PHARMEASY\' WHEN upper(trim(\"Portal Name\")) LIKE \'%MEESHO%\' THEN \'MEESHO\' WHEN upper(trim(\"Portal Name\")) LIKE \'%SHOPCLUES%\' THEN \'SHOPCLUES\' WHEN upper(trim(\"Portal Name\")) LIKE \'%FIRSTCRY%\' THEN \'FIRSTCRY\' WHEN upper(trim(\"Portal Name\")) LIKE \'%ONDCCOSTBO%\' THEN \'ONDC\' ELSE upper(trim(\"Portal Name\")) END AS FINAL_MARKETPLACE, upper(trim(\"Market Place SKU\")) as channel_sku, upper(trim(im.name)) as product_name, upper(trim(asin)) as product_id, ifnull(cast(trim(\"combo units\") as int),0) as combo_units, cast(trim(\"Selling price\") as float) as selling_price, upper(trim(\"Brand name\")) as brand, upper(trim(\"Category \")) as category, upper(trim(\"sub category\")) as sub_category, upper(trim(combo)) as is_combo, case when trim(\"Packaging Cost\") = \'\' then null else cast(ifnull(cast(trim(\"Packaging Cost\") as float),0) as float) end as packaging_cost, cast(trim(\"Item code\") as varchar) as item_code, case when trim(\"Pack Size\") = \'NA\' then null else cast(trim(\"Pack Size\") as int) end as pack_size, case when upper(trim(nlc)) = \'NA\' then null else cast(replace(trim(nlc),\',\',\'\') as float) end as landing_cost, trim(\"HSN code\") as hsn_code, case when \"Tax Rate\" = \'\' or \"Tax Rate\" = \'NA\' then null else div0(replace(trim(\"Tax Rate\"),\'%\',\'\'),100) end as tax_rate, case when upper(coo) like \'CHIINA\' then \'CHINA\' when upper(coo) like \'VITNAM\' then \'VIETNAM\' else upper(coo) end as country_of_origin from medmongers_db.maplemonk.medmongers_google_sheets_sku_master sm LEFT JOIN ( select \"Product Code\" AS SKU_CODE, MRP, NAME, SIZE, \"Cost Price\", \"Category Code\", \"Category Name\", upper(Brand) brand from maplemonk.medmongers_get_item_master qualify row_number() over (partition by upper(trim(\"Product Code\")) order by 1 desc)=1 ) im on upper(cast(trim(sm.\"TALLY SKU\") as varchar)) = upper(trim(im.sku_code)) qualify row_number() over (partition by upper(cast(trim(\"TALLY SKU\") as varchar)), upper(trim(\"Portal Name\")),upper(trim(\"Market Place SKU\")) order by 1 desc)=1 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from MEDMONGERS_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            